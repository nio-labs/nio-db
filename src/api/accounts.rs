use super::*;
use crate::storage::{UserRecord, UserSession, hex};
use hmac::Hmac;
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, sync::LazyLock, time::Instant};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

static PASSWORD_CAPACITY: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
static LOGIN_ATTEMPTS: LazyLock<Mutex<VecDeque<Instant>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));
const ROUNDS: u32 = 600_000;

#[derive(Clone)]
pub(super) struct SessionIdentity {
    pub user: UserRecord,
    pub hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credentials {
    username: String,
    password: String,
}
fn username(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    ((3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.'))
    .then_some(value)
}
fn public_user(user: &UserRecord) -> Value {
    json!({"id":user.id,"username":user.username,"created_at":user.created_at})
}
fn derive(password: &str, salt: &[u8]) -> [u8; 32] {
    let mut output = [0u8; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), salt, ROUNDS, &mut output);
    output
}
fn hash_password(password: &str) -> String {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    format!(
        "pbkdf2-sha256${ROUNDS}${}${}",
        hex(&salt),
        hex(&derive(password, &salt))
    )
}
fn unhex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|s| u8::from_str_radix(s, 16).ok())
        })
        .collect()
}
fn verify_password(password: &str, stored: Option<&str>) -> bool {
    let dummy = format!(
        "pbkdf2-sha256${ROUNDS}${}${}",
        "00".repeat(16),
        "00".repeat(32)
    );
    let parts: Vec<_> = stored.unwrap_or(&dummy).split('$').collect();
    if parts.len() != 4 || parts[0] != "pbkdf2-sha256" || parts[1] != ROUNDS.to_string() {
        return false;
    }
    let Some(salt) = unhex(parts[2]).filter(|s| s.len() == 16) else {
        return false;
    };
    let Some(expected) = unhex(parts[3]).filter(|h| h.len() == 32) else {
        return false;
    };
    bool::from(derive(password, &salt).ct_eq(&expected)) && stored.is_some()
}
async fn password_work<T: Send + 'static>(
    id: &RequestId,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ApiError> {
    let permit = PASSWORD_CAPACITY.clone().try_acquire_owned().map_err(|_| {
        error(
            id,
            StatusCode::TOO_MANY_REQUESTS,
            "auth_busy",
            "Authentication capacity is occupied",
        )
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| {
        error(
            id,
            StatusCode::SERVICE_UNAVAILABLE,
            "auth_unavailable",
            "Authentication is unavailable",
        )
    })
}

pub(super) async fn register(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<SessionIdentity>>,
    body: Result<Json<Credentials>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if session.is_some() {
        return Err(error(
            &id,
            StatusCode::FORBIDDEN,
            "backend_required",
            "A backend server credential is required to create users",
        ));
    }
    let body = body.map_err(|r| body_error(&id, r))?.0;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let username = username(&body.username).ok_or_else(|| {
        error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_username",
            "Username must contain 3 to 64 ASCII letters, digits, dots, hyphens or underscores",
        )
    })?;
    if !(12..=1024).contains(&body.password.len()) {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_password",
            "Password must contain 12 to 1024 UTF-8 bytes",
        ));
    }
    let hash = password_work(&id, move || hash_password(&body.password)).await?;
    let user = storage(&app, &id, move |store| {
        store.create_user(workspace, username, hash)
    })
    .await
    .map_err(|e| {
        if e.code == "idempotency_conflict" {
            error(
                &id,
                StatusCode::CONFLICT,
                "username_taken",
                "Username is already registered in this workspace",
            )
        } else {
            e
        }
    })?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"user":public_user(&user)})),
    ))
}

pub(super) async fn login(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    body: Result<Json<Credentials>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    {
        let mut attempts = LOGIN_ATTEMPTS.lock().map_err(|_| {
            error(
                &id,
                StatusCode::SERVICE_UNAVAILABLE,
                "auth_unavailable",
                "Authentication is unavailable",
            )
        })?;
        attempts.retain(|time| time.elapsed().as_secs() < 60);
        if attempts.len() >= 30 {
            return Err(error(
                &id,
                StatusCode::TOO_MANY_REQUESTS,
                "login_rate_limited",
                "Too many login attempts; retry in a minute",
            ));
        }
        attempts.push_back(Instant::now());
    }
    let body = body.map_err(|r| body_error(&id, r))?.0;
    let workspace = server_workspace(&app);
    let invalid = || {
        error(
            &id,
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "Invalid username or password",
        )
    };
    let username = username(&body.username).ok_or_else(invalid)?;
    if body.password.is_empty() || body.password.len() > 1024 {
        return Err(invalid());
    }
    let user = storage(&app, &id, move |store| {
        Ok(store.find_user(&workspace, &username))
    })
    .await?;
    let hash = user.as_ref().map(|u| u.password_hash.clone());
    if !password_work(&id, move || {
        verify_password(&body.password, hash.as_deref())
    })
    .await?
    {
        return Err(invalid());
    }
    let user = user.ok_or_else(invalid)?;
    let mut random = [0u8; 32];
    OsRng.fill_bytes(&mut random);
    let token = format!("nio_session_{}", hex(&random));
    let expires_at = chrono::Utc::now().timestamp() + 7 * 24 * 3600;
    let session = UserSession {
        token_sha256: hex(&Sha256::digest(token.as_bytes())),
        user_id: user.id.clone(),
        expires_at,
    };
    storage(&app, &id, move |store| store.create_session(session)).await?;
    Ok(Json(
        json!({"access_token":token,"token_type":"Bearer","expires_in":7*24*3600,"expires_at":expires_at,"user":public_user(&user)}),
    ))
}

pub(super) async fn me(
    Extension(id): Extension<RequestId>,
    session: Option<Extension<SessionIdentity>>,
) -> Result<Json<Value>, ApiError> {
    let session = session.ok_or_else(|| {
        error(
            &id,
            StatusCode::FORBIDDEN,
            "user_session_required",
            "A logged-in user session is required",
        )
    })?;
    Ok(Json(json!({"user":public_user(&session.user)})))
}
pub(super) async fn logout(
    State(app): State<App>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<SessionIdentity>>,
) -> Result<StatusCode, ApiError> {
    let session = session.ok_or_else(|| {
        error(
            &id,
            StatusCode::FORBIDDEN,
            "user_session_required",
            "A logged-in user session is required",
        )
    })?;
    storage(&app, &id, move |store| {
        store.revoke_session(session.hash.clone())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn delete_user(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<SessionIdentity>>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if session.is_some() {
        return Err(error(
            &id,
            StatusCode::FORBIDDEN,
            "backend_required",
            "A backend server credential is required to delete users",
        ));
    }
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let deleted = storage(&app, &id, move |store| {
        store.delete_user(&workspace, &user_id)
    })
    .await?;
    if !deleted {
        return Err(error(
            &id,
            StatusCode::NOT_FOUND,
            "not_found",
            "User not found",
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_users(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<SessionIdentity>>,
) -> Result<Json<Value>, ApiError> {
    if session.is_some() {
        return Err(error(
            &id,
            StatusCode::FORBIDDEN,
            "backend_required",
            "A backend server credential is required to list users",
        ));
    }
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let users = storage(&app, &id, move |store| {
        Ok(store.users(&workspace))
    })
    .await?;
    let public: Vec<_> = users.iter().map(|u| public_user(u)).collect();
    Ok(Json(json!({"users": public})))
}
