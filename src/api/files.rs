use super::*;
use crate::storage::{StoredFile, UploadTicket, hex, valid_bucket};
use axum::extract::{FromRequest, Multipart};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::{sync::LazyLock, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Semaphore,
};
use tokio_util::io::ReaderStream;

fn validate_bucket(name: &str, id: &RequestId) -> Result<(), ApiError> {
    if valid_bucket(name) {
        Ok(())
    } else {
        Err(error(
            id,
            StatusCode::BAD_REQUEST,
            "invalid_bucket",
            "Bucket names use 1 to 63 lowercase ASCII letters, digits, hyphens or underscores",
        ))
    }
}
fn valid_filename(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
        && name != "."
        && name != ".."
}
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
static UPLOADS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BucketInput {
    #[serde(skip)]
    workspace_id: String,
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Workspace {
    #[serde(skip)]
    workspace_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListFiles {
    #[serde(skip)]
    workspace_id: String,
    #[serde(default = "default_limit")]
    limit: usize,
    cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UploadQuery {
    #[serde(skip)]
    workspace_id: String,
    filename: Option<String>,
    user_id: Option<String>,
}

fn visible(file: &StoredFile, user_id: Option<&str>) -> bool {
    file.user_id
        .as_deref()
        .is_none_or(|owner| Some(owner) == user_id)
}

pub(super) async fn buckets(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    query: Result<Query<Workspace>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid request parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    authorize(&principal, &query.workspace_id, &id)?;
    let items = storage(&app, &id, move |s| Ok(s.buckets(&query.workspace_id))).await?;
    Ok(Json(
        json!({"items":items.iter().map(public_value).collect::<Vec<_>>()}),
    ))
}
pub(super) async fn create_bucket(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    body: Result<Json<BucketInput>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut body = body.map_err(|r| body_error(&id, r))?.0;
    body.workspace_id = server_workspace(&app);
    authorize(&principal, &body.workspace_id, &id)?;
    validate_bucket(&body.name, &id)?;
    let bucket = storage(&app, &id, move |s| {
        s.create_bucket(&body.workspace_id, &body.name, &principal.name)
    })
    .await?;
    Ok((StatusCode::CREATED, Json(public_value(&bucket))))
}
pub(super) async fn list(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(bucket): Path<String>,
    query: Result<Query<ListFiles>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid file list parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    authorize(&principal, &query.workspace_id, &id)?;
    validate_bucket(&bucket, &id)?;
    if !(1..=1000).contains(&query.limit) {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Limit must be 1 to 1000",
        ));
    }
    let mut files = storage(
        &app,
        &id,
        move |s| Ok(s.files(&query.workspace_id, &bucket)),
    )
    .await?;
    files.retain(|file| visible(file, session.as_ref().map(|s| s.user.id.as_str())));
    files.sort_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
    if let Some(cursor) = query.cursor {
        let index = files.iter().position(|f| f.id == cursor).ok_or_else(|| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "Cursor does not belong to this bucket",
            )
        })?;
        files.drain(..=index);
    }
    let more = files.len() > query.limit;
    files.truncate(query.limit);
    let next = if more {
        files.last().map(|f| f.id.clone())
    } else {
        None
    };
    Ok(Json(
        json!({"items":files.iter().map(public_value).collect::<Vec<_>>(),"next_cursor":next}),
    ))
}
async fn find(
    app: &App,
    id: &RequestId,
    principal: &Principal,
    user_id: Option<&str>,
    bucket: String,
    file_id: String,
    workspace: String,
) -> Result<StoredFile, ApiError> {
    authorize(principal, &workspace, id)?;
    validate_bucket(&bucket, id)?;
    let user_id = user_id.map(str::to_owned);
    storage(app, id, move |s| {
        Ok(s.file(&workspace, &bucket, &file_id)
            .filter(|file| visible(file, user_id.as_deref())))
    })
    .await?
    .ok_or_else(|| {
        error(
            id,
            StatusCode::NOT_FOUND,
            "not_found",
            "File not found in this bucket",
        )
    })
}
pub(super) async fn metadata(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path((bucket, file_id)): Path<(String, String)>,
    query: Result<Query<Workspace>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid request parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    Ok(Json(public_value(
        &find(
            &app,
            &id,
            &principal,
            session.as_ref().map(|s| s.user.id.as_str()),
            bucket,
            file_id,
            query.workspace_id,
        )
        .await?,
    )))
}
pub(super) async fn delete(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path((bucket, file_id)): Path<(String, String)>,
    query: Result<Query<Workspace>, QueryRejection>,
) -> Result<StatusCode, ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid request parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    let file = find(
        &app,
        &id,
        &principal,
        session.as_ref().map(|s| s.user.id.as_str()),
        bucket,
        file_id,
        query.workspace_id,
    )
    .await?;
    storage(&app, &id, move |s| s.delete_file(file)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn write_chunk(
    file: &mut tokio::fs::File,
    hash: &mut Sha256,
    size: &mut u64,
    chunk: &[u8],
    id: &RequestId,
) -> Result<(), ApiError> {
    *size += chunk.len() as u64;
    if *size > MAX_FILE_BYTES {
        return Err(error(
            id,
            StatusCode::PAYLOAD_TOO_LARGE,
            "file_too_large",
            "Files must be at most 64 MiB",
        ));
    }
    hash.update(chunk);
    file.write_all(chunk).await.map_err(|_| {
        error(
            id,
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "File write failed",
        )
    })
}
async fn receive(
    app: &App,
    id: &RequestId,
    ticket: &UploadTicket,
    request: Request<Body>,
    filename: Option<String>,
) -> Result<(String, String, u64, String), ApiError> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let mut file = tokio::fs::File::from_std(ticket.file.try_clone().map_err(|_| {
        error(
            id,
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "File write failed",
        )
    })?);
    let mut hash = Sha256::new();
    let mut size = 0;
    let (filename, mime) = if content_type
        .to_ascii_lowercase()
        .starts_with("multipart/form-data")
    {
        let mut multipart = Multipart::from_request(request, app).await.map_err(|_| {
            error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_upload",
                "Expected multipart/form-data with one file field",
            )
        })?;
        let mut field = multipart
            .next_field()
            .await
            .map_err(|_| {
                error(
                    id,
                    StatusCode::BAD_REQUEST,
                    "invalid_upload",
                    "Invalid multipart upload",
                )
            })?
            .ok_or_else(|| {
                error(
                    id,
                    StatusCode::BAD_REQUEST,
                    "invalid_upload",
                    "A file field is required",
                )
            })?;
        if field.name() != Some("file") {
            return Err(error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_upload",
                "The upload field must be named file",
            ));
        }
        let filename = field.file_name().map(str::to_owned).ok_or_else(|| {
            error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_filename",
                "A filename is required",
            )
        })?;
        let mime = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        if !valid_filename(&filename) {
            return Err(error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_filename",
                "Use a filename without directory separators or control characters",
            ));
        }
        while let Some(chunk) = field.chunk().await.map_err(|_| {
            error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_upload",
                "Invalid or oversized upload",
            )
        })? {
            write_chunk(&mut file, &mut hash, &mut size, &chunk, id).await?;
        }
        if multipart
            .next_field()
            .await
            .map_err(|_| {
                error(
                    id,
                    StatusCode::BAD_REQUEST,
                    "invalid_upload",
                    "Invalid multipart upload",
                )
            })?
            .is_some()
        {
            return Err(error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_upload",
                "Upload exactly one file per request",
            ));
        }
        (filename, mime)
    } else {
        let filename = filename.filter(|f| valid_filename(f)).ok_or_else(|| {
            error(
                id,
                StatusCode::BAD_REQUEST,
                "invalid_filename",
                "Raw uploads require a filename query parameter without directory separators",
            )
        })?;
        let mut stream = request.into_body().into_data_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                error(
                    id,
                    StatusCode::BAD_REQUEST,
                    "invalid_upload",
                    "Upload stream failed",
                )
            })?;
            write_chunk(&mut file, &mut hash, &mut size, &chunk, id).await?;
        }
        (filename, content_type)
    };
    if mime.len() > 127
        || mime.parse::<mime::Mime>().is_err()
        || mime.bytes().any(|b| b.is_ascii_control())
    {
        return Err(error(
            id,
            StatusCode::BAD_REQUEST,
            "invalid_mime",
            "Invalid content type",
        ));
    }
    file.sync_all().await.map_err(|_| {
        error(
            id,
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "File sync failed",
        )
    })?;
    Ok((filename, mime, size, hex(&hash.finalize())))
}
pub(super) async fn upload(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(bucket): Path<String>,
    query: Result<Query<UploadQuery>, QueryRejection>,
    request: Request<Body>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid upload parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    authorize(&principal, &query.workspace_id, &id)?;
    validate_bucket(&bucket, &id)?;
    if let Some(owner) = query.user_id.as_deref() {
        if session.as_ref().is_some_and(|s| s.user.id != owner) {
            return Err(error(
                &id,
                StatusCode::FORBIDDEN,
                "forbidden",
                "Files can only be linked to your own user ID",
            ));
        }
        let workspace = query.workspace_id.clone();
        let owner = owner.to_owned();
        let exists = storage(&app, &id, move |s| Ok(s.user_exists(&workspace, &owner))).await?;
        if !exists {
            return Err(error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_user_id",
                "The user ID does not exist",
            ));
        }
    }
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > 65 * 1024 * 1024)
    {
        return Err(error(
            &id,
            StatusCode::PAYLOAD_TOO_LARGE,
            "file_too_large",
            "Files must be at most 64 MiB",
        ));
    }
    let _permit = UPLOADS.clone().try_acquire_owned().map_err(|_| {
        error(
            &id,
            StatusCode::TOO_MANY_REQUESTS,
            "upload_busy",
            "Upload capacity is occupied",
        )
    })?;
    let ticket = storage(&app, &id, |s| s.begin_upload()).await?;
    let (filename, mime, size, sha256) = tokio::time::timeout(
        Duration::from_secs(60),
        receive(&app, &id, &ticket, request, query.filename),
    )
    .await
    .map_err(|_| {
        error(
            &id,
            StatusCode::REQUEST_TIMEOUT,
            "upload_timeout",
            "Upload timed out",
        )
    })??;
    let file = StoredFile {
        id: new_id("file"),
        artifact_id: String::new(),
        workspace_id: query.workspace_id,
        bucket,
        filename,
        sha256,
        size,
        mime,
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        created_by: principal.name,
        user_id: query.user_id,
    };
    let file = storage(&app, &id, move |s| s.finish_upload(file, &ticket))
        .await
        .map_err(|e| {
            if e.code == "record_too_large" {
                error(
                    &id,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "storage_quota_exceeded",
                    "Workspace or server file quota exceeded",
                )
            } else {
                e
            }
        })?;
    Ok((StatusCode::CREATED, Json(public_value(&file))))
}
fn range(value: &str, size: u64) -> Option<(u64, u64)> {
    let value = value.strip_prefix("bytes=")?;
    if value.contains(',') || size == 0 {
        return None;
    }
    let (start, end) = value.split_once('-')?;
    if start.is_empty() {
        let length = end.parse::<u64>().ok()?;
        if length == 0 {
            return None;
        }
        return Some((size.saturating_sub(length), size - 1));
    }
    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().ok()?.min(size - 1)
    };
    (start <= end && start < size).then_some((start, end))
}
fn encoded_name(name: &str) -> String {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
pub(super) async fn download(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path((bucket, file_id)): Path<(String, String)>,
    query: Result<Query<Workspace>, QueryRejection>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let mut query = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid request parameters",
            )
        })?
        .0;
    query.workspace_id = server_workspace(&app);
    let metadata = find(
        &app,
        &id,
        &principal,
        session.as_ref().map(|s| s.user.id.as_str()),
        bucket,
        file_id,
        query.workspace_id,
    )
    .await?;
    let etag = format!("\"{}\"", metadata.sha256);
    let requested = headers.get(header::RANGE).and_then(|h| h.to_str().ok());
    let if_range = headers.get(header::IF_RANGE).and_then(|h| h.to_str().ok());
    let requested = requested.filter(|_| if_range.is_none_or(|r| r == etag));
    let selected = if let Some(value) = requested {
        match range(value, metadata.size) {
            Some(r) => Some(r),
            None => {
                let mut response = error(
                    &id,
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "invalid_range",
                    "Requested byte range is not satisfiable",
                )
                .into_response();
                response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes */{}", metadata.size)).unwrap(),
                );
                return Ok(response);
            }
        }
    } else {
        None
    };
    let (start, length) = selected
        .map(|(a, b)| (a, b - a + 1))
        .unwrap_or((0, metadata.size));
    let info = metadata.clone();
    // Open while holding the store lock so a concurrent deletion cannot race the open.
    let file = storage(&app, &id, move |s| std::fs::File::open(s.blob_path(&info)?)).await?;
    let mut file = tokio::fs::File::from_std(file);
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|_| {
            error(
                &id,
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
                "Download is unavailable",
            )
        })?;
    let mut response = Body::from_stream(ReaderStream::new(file.take(length))).into_response();
    *response.status_mut() = if selected.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let output = response.headers_mut();
    output.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&metadata.mime).unwrap(),
    );
    output.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).unwrap(),
    );
    output.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename*=UTF-8''{}",
            encoded_name(&metadata.filename)
        ))
        .unwrap(),
    );
    output.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    output.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
    if let Some((a, b)) = selected {
        output.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {a}-{b}/{}", metadata.size)).unwrap(),
        );
    }
    Ok(response)
}
