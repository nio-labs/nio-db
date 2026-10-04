use super::*;
use crate::storage::{EventWorker, Webhook, hex};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use futures_util::stream;
use hmac::{Hmac, Mac};
use rand::{RngCore, rngs::OsRng};
use sha2::Sha256;
use std::{
    collections::VecDeque,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, process::Command, sync::Semaphore};

static WORKER_CAPACITY: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
static WEBHOOK_CAPACITY: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(8)));
static PUBLISHES: LazyLock<Mutex<VecDeque<Instant>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

fn valid_name(value: &str, wildcard: bool) -> bool {
    (wildcard && value == "*")
        || (!value.is_empty()
            && value.len() <= 64
            && value.as_bytes()[0].is_ascii_alphabetic()
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)))
}

fn admin(
    session: Option<Extension<accounts::SessionIdentity>>,
    id: &RequestId,
) -> Result<(), ApiError> {
    if session.is_some() {
        return Err(error(
            id,
            StatusCode::FORBIDDEN,
            "backend_required",
            "A backend server credential is required",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publish {
    #[serde(alias = "type")]
    name: String,
    data: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateWorker {
    #[serde(alias = "event_type")]
    event_name: String,
    source: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateWebhook {
    #[serde(alias = "event_type")]
    event_name: String,
    url: String,
}

fn public_worker(worker: &EventWorker) -> Value {
    json!({"id":worker.id,"event_name":worker.event_name,"created_at":worker.created_at,"created_by":worker.created_by})
}
fn public_webhook(webhook: &Webhook) -> Value {
    json!({"id":webhook.id,"event_name":webhook.event_name,"url":webhook.url,"created_at":webhook.created_at,"created_by":webhook.created_by})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StreamQuery {
    name: Option<String>,
}

pub(super) async fn stream(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<
    Sse<impl futures_util::Stream<Item = Result<SseEvent, std::convert::Infallible>>>,
    ApiError,
> {
    let filter = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid event subscription",
            )
        })?
        .0
        .name;
    if filter
        .as_deref()
        .is_some_and(|name| !valid_name(name, true))
    {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_event_name",
            "Invalid event name",
        ));
    }
    let filter = filter.filter(|name| name != "*");
    let receiver = app.events.subscribe();
    let stream = stream::unfold((receiver, filter), |(mut receiver, filter)| async move {
        loop {
            match receiver.recv().await {
                Ok(value)
                    if filter
                        .as_ref()
                        .is_some_and(|name| value["name"].as_str() != Some(name)) =>
                {
                    continue;
                }
                Ok(value) => {
                    return Some((
                        Ok(SseEvent::default().event("message").data(value.to_string())),
                        (receiver, filter),
                    ));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    return Some((
                        Ok(SseEvent::default()
                            .event("gap")
                            .data(json!({"missed":count}).to_string())),
                        (receiver, filter),
                    ));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

pub(super) async fn publish(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    body: Result<Json<Publish>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let body = body.map_err(|r| body_error(&id, r))?.0;
    if !valid_name(&body.name, false) {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_event_name",
            "Event name must start with a letter and use up to 64 ASCII letters, digits, dots, underscores or hyphens",
        ));
    }
    if serde_json::to_vec(&body.data).is_ok_and(|bytes| bytes.len() > 16 * 1024) {
        return Err(error(
            &id,
            StatusCode::PAYLOAD_TOO_LARGE,
            "event_too_large",
            "Event data must fit 16 KiB",
        ));
    }
    {
        let mut recent = PUBLISHES.lock().map_err(|_| {
            error(
                &id,
                StatusCode::SERVICE_UNAVAILABLE,
                "event_unavailable",
                "Event publishing is unavailable",
            )
        })?;
        recent.retain(|time| time.elapsed() < Duration::from_secs(1));
        if recent.len() >= 100 {
            return Err(error(
                &id,
                StatusCode::TOO_MANY_REQUESTS,
                "event_rate_limited",
                "Event publishing is limited to 100 events per second",
            ));
        }
        recent.push_back(Instant::now());
    }
    let event = json!({"id":new_id("evt"),"name":body.name,"data":body.data,"actor":principal.name,"created_at":chrono::Utc::now().to_rfc3339()});
    let name = event["name"].as_str().unwrap().to_owned();
    let (workers, webhooks) = storage(&app, &id, move |store| {
        Ok((
            store
                .event_workers()
                .into_iter()
                .filter(|w| w.event_name == "*" || w.event_name == name)
                .collect::<Vec<_>>(),
            store
                .webhooks()
                .into_iter()
                .filter(|w| w.event_name == "*" || w.event_name == name)
                .collect::<Vec<_>>(),
        ))
    })
    .await?;
    let _ = app.events.send(event.clone());
    for worker in workers {
        let event = event.clone();
        let node = app.node_binary.clone();
        let runner = app.worker_runner.clone();
        tokio::spawn(async move {
            run_worker(worker, event, node, runner).await;
        });
    }
    for webhook in webhooks {
        let event = event.clone();
        tokio::spawn(async move {
            send_webhook(webhook, event).await;
        });
    }
    Ok((StatusCode::ACCEPTED, Json(event)))
}

async fn run_worker(
    worker: EventWorker,
    event: Value,
    node: std::path::PathBuf,
    runner: std::path::PathBuf,
) {
    let Ok(_permit) = WORKER_CAPACITY.clone().try_acquire_owned() else {
        eprintln!("event worker {} skipped: capacity full", worker.id);
        return;
    };
    let payload = json!({"source":worker.source,"event":event}).to_string();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut child = Command::new(node)
            .arg("--max-old-space-size=64")
            .arg(runner)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(payload.as_bytes()).await?;
        }
        child.wait().await
    })
    .await;
    if !matches!(result, Ok(Ok(status)) if status.success()) {
        eprintln!("event worker {} failed or timed out", worker.id);
    }
}

async fn send_webhook(webhook: Webhook, event: Value) {
    let Ok(_permit) = WEBHOOK_CAPACITY.clone().try_acquire_owned() else {
        eprintln!("webhook {} skipped: capacity full", webhook.id);
        return;
    };
    let bytes = event.to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(webhook.secret.as_bytes()).unwrap();
    mac.update(bytes.as_bytes());
    let signature = format!("sha256={}", hex(&mac.finalize().into_bytes()));
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    for attempt in 0..3 {
        if let Ok(response) = client
            .post(&webhook.url)
            .header("Content-Type", "application/json")
            .header("X-NioDB-Event", event["name"].as_str().unwrap_or(""))
            .header("X-NioDB-Delivery", event["id"].as_str().unwrap_or(""))
            .header("X-NioDB-Signature", &signature)
            .body(bytes.clone())
            .send()
            .await
            && response.status().is_success()
        {
            return;
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
        }
    }
    eprintln!(
        "webhook {} delivery failed for event {}",
        webhook.id, event["id"]
    );
}

pub(super) async fn workers(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
) -> Result<Json<Value>, ApiError> {
    admin(session, &id)?;
    let items = storage(&app, &id, |store| Ok(store.event_workers())).await?;
    Ok(Json(
        json!({"items":items.iter().map(public_worker).collect::<Vec<_>>()}),
    ))
}

pub(super) async fn create_worker(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    body: Result<Json<CreateWorker>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    admin(session, &id)?;
    let body = body.map_err(|r| body_error(&id, r))?.0;
    if !valid_name(&body.event_name, true)
        || body.source.is_empty()
        || body.source.len() > 32 * 1024
    {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_worker",
            "Provide an event name and up to 32 KiB of TypeScript source",
        ));
    }
    let ready = if app.worker_runner.is_file() {
        tokio::time::timeout(
            Duration::from_secs(2),
            Command::new(&app.node_binary)
                .arg("-p")
                .arg("typeof require('node:module').stripTypeScriptTypes")
                .output(),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .is_some_and(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "function"
        })
    } else {
        false
    };
    if !ready {
        return Err(error(
            &id,
            StatusCode::SERVICE_UNAVAILABLE,
            "worker_unavailable",
            "TypeScript event workers require Node 22+ and the NioDB worker runner",
        ));
    }
    let worker = EventWorker {
        id: new_id("wrk"),
        event_name: body.event_name,
        source: body.source,
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by: principal.name,
    };
    let result = public_worker(&worker);
    storage(&app, &id, move |store| store.create_event_worker(worker)).await?;
    Ok((StatusCode::CREATED, Json(result)))
}

pub(super) async fn delete_worker(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(worker_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    admin(session, &id)?;
    if !storage(&app, &id, move |store| {
        store.delete_event_worker(&worker_id)
    })
    .await?
    {
        return Err(error(
            &id,
            StatusCode::NOT_FOUND,
            "not_found",
            "Event worker not found",
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn webhooks(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
) -> Result<Json<Value>, ApiError> {
    admin(session, &id)?;
    let items = storage(&app, &id, |store| Ok(store.webhooks())).await?;
    Ok(Json(
        json!({"items":items.iter().map(public_webhook).collect::<Vec<_>>()}),
    ))
}

pub(super) async fn create_webhook(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    body: Result<Json<CreateWebhook>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    admin(session, &id)?;
    let body = body.map_err(|r| body_error(&id, r))?.0;
    let url = reqwest::Url::parse(&body.url).map_err(|_| {
        error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_url",
            "Webhook URL is invalid",
        )
    })?;
    let loopback = url.host_str().is_some_and(|host| {
        matches!(
            host.trim_matches(['[', ']']),
            "localhost" | "127.0.0.1" | "::1"
        )
    });
    if !valid_name(&body.event_name, true)
        || body.url.len() > 2048
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_webhook",
            "Use an event name and an HTTPS URL (HTTP is allowed for loopback) without credentials or fragments",
        ));
    }
    let mut random = [0u8; 32];
    OsRng.fill_bytes(&mut random);
    let webhook = Webhook {
        id: new_id("whk"),
        event_name: body.event_name,
        url: body.url,
        secret: hex(&random),
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by: principal.name,
    };
    let mut result = public_webhook(&webhook);
    result["secret"] = json!(webhook.secret);
    storage(&app, &id, move |store| store.create_webhook(webhook)).await?;
    Ok((StatusCode::CREATED, Json(result)))
}

pub(super) async fn delete_webhook(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(webhook_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    admin(session, &id)?;
    if !storage(&app, &id, move |store| store.delete_webhook(&webhook_id)).await? {
        return Err(error(
            &id,
            StatusCode::NOT_FOUND,
            "not_found",
            "Webhook not found",
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}
