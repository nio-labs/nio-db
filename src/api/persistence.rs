//! Backend credential APIs for application-owned durable storage.
use super::*;
use crate::storage::{CollectionConstraints, MutationRequest};

fn backend_only(
    id: &RequestId,
    session: &Option<Extension<accounts::SessionIdentity>>,
) -> Result<(), ApiError> {
    if session.is_some() {
        return Err(error(
            id,
            StatusCode::FORBIDDEN,
            "backend_only",
            "Backend credentials are required",
        ));
    }
    Ok(())
}

pub(super) fn storage_error(
    err: &std::io::Error,
) -> Option<(StatusCode, &'static str, &'static str)> {
    let code = err.to_string();
    Some(match code.as_str() {
        "unique_conflict" => (
            StatusCode::CONFLICT,
            "unique_conflict",
            "Application key already exists",
        ),
        "reference_conflict" => (
            StatusCode::CONFLICT,
            "reference_conflict",
            "Reference constraint would be violated",
        ),
        "revision_conflict" => (
            StatusCode::CONFLICT,
            "revision_conflict",
            "Record revision changed",
        ),
        "constraint_conflict" => (
            StatusCode::CONFLICT,
            "constraint_conflict",
            "Collection policy is already installed",
        ),
        "mutation_conflict" => (
            StatusCode::CONFLICT,
            "mutation_conflict",
            "Mutation key was reused with different operations",
        ),
        "lease_conflict" => (
            StatusCode::CONFLICT,
            "lease_conflict",
            "Lease expired or is owned by another run",
        ),
        "lease_required" => (
            StatusCode::CONFLICT,
            "lease_required",
            "Mutation requires a valid lease proof",
        ),
        "invalid_constraints" => (
            StatusCode::BAD_REQUEST,
            "invalid_constraints",
            "Invalid collection constraints or nonunique reference target",
        ),
        "invalid_constraint_value" => (
            StatusCode::BAD_REQUEST,
            "invalid_constraint_value",
            "Constrained fields require nonempty bounded strings",
        ),
        "constraint_ttl" => (
            StatusCode::BAD_REQUEST,
            "constraint_ttl",
            "TTL is unsupported for constrained records and atomic mutations",
        ),
        "invalid_mutation" => (
            StatusCode::BAD_REQUEST,
            "invalid_mutation",
            "Invalid mutation or operation limit exceeded",
        ),
        "invalid_lease" => (
            StatusCode::BAD_REQUEST,
            "invalid_lease",
            "Invalid lease resource, owner or duration",
        ),
        "mutation_not_found" => (
            StatusCode::NOT_FOUND,
            "mutation_not_found",
            "Mutation record not found",
        ),
        "mutation_too_large" => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "mutation_too_large",
            "Mutation request or result exceeds 8 MiB",
        ),
        "constraint_limit" => (
            StatusCode::SERVICE_UNAVAILABLE,
            "constraint_limit",
            "Collection policy capacity reached",
        ),
        "lease_limit" => (
            StatusCode::SERVICE_UNAVAILABLE,
            "lease_limit",
            "Lease resource capacity reached",
        ),
        "receipt_limit" => (
            StatusCode::SERVICE_UNAVAILABLE,
            "receipt_limit",
            "Mutation receipt capacity reached",
        ),
        "mutation_expired" => (StatusCode::CONFLICT, "mutation_expired", "Mutation key has expired"),
        "deletion_in_progress" => (StatusCode::CONFLICT, "deletion_in_progress", "Record is being deleted"),
        "deletion_job_conflict" | "deletion_cycle" | "deletion_job_file" => (StatusCode::CONFLICT, "deletion_job_conflict", "Deletion job conflicts with existing records"),
        "deletion_job_not_found" => (StatusCode::NOT_FOUND, "deletion_job_not_found", "Deletion job or root was not found"),
        "invalid_deletion_job" => (StatusCode::BAD_REQUEST, "invalid_deletion_job", "Invalid deletion job"),
        "deletion_job_too_large" => (StatusCode::PAYLOAD_TOO_LARGE, "deletion_job_too_large", "Deletion plan exceeds capacity"),
        "deletion_job_limit" => (StatusCode::SERVICE_UNAVAILABLE, "deletion_job_limit", "Too many pending deletion jobs"),
        _ => return None,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeletionJobRequest {
    root_id: String,
    idempotency_key: String,
}

pub(super) async fn start_deletion_job(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    body: Result<Json<DeletionJobRequest>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let request = body.map_err(|r| body_error(&id, r))?.0;
    let scope = principal.name.clone();
    let job = storage(&app, &id, move |store| {
        store.start_deletion_job(&workspace, &scope, &request.idempotency_key, &request.root_id)
    }).await?;
    if job.status == "pending" {
        spawn_deletion_worker(app, job.id.clone());
    }
    Ok(Json(json!({"job":job})))
}

pub(super) async fn deletion_job_status(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let job = storage(&app, &id, move |store| {
        store.deletion_job(&workspace, &job_id)
            .ok_or_else(|| std::io::Error::other("deletion_job_not_found"))
    }).await?;
    Ok(Json(json!({"job":job})))
}

pub(super) async fn persistence_status(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let status = storage(&app, &id, move |store| Ok(store.persistence_status())).await?;
    Ok(Json(status))
}

fn spawn_deletion_worker(app: App, job_id: String) {
    tokio::spawn(async move {
        loop {
            let workspace = server_workspace(&app);
            let job = job_id.clone();
            let result = storage(&app, &RequestId("deletion-worker".into()), move |store| {
                store.advance_deletion_job(&workspace, &job)
            }).await;
            match result {
                Ok(status) if status.status == "pending" => tokio::task::yield_now().await,
                _ => break,
            }
        }
    });
}

pub fn resume_deletion_jobs(app: App) {
    let jobs = app.store.lock().unwrap().pending_deletion_jobs();
    for job_id in jobs {
        spawn_deletion_worker(app.clone(), job_id);
    }
}

pub(super) async fn configure(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(collection): Path<String>,
    body: Result<Json<CollectionConstraints>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let policy = body.map_err(|r| body_error(&id, r))?.0;
    let output = policy.clone();
    storage(&app, &id, move |store| {
        store.configure_collection(&workspace, collection, policy)
    })
    .await?;
    Ok(Json(json!({"constraints": output})))
}

pub(super) async fn constraints(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    Path(collection): Path<String>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let output = storage(&app, &id, move |store| {
        Ok(store.collection_constraints(&workspace, &collection))
    })
    .await?;
    Ok(Json(json!({"constraints": output})))
}

pub(super) async fn mutate(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    body: Result<Json<MutationRequest>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let request = body.map_err(|r| body_error(&id, r))?.0;
    let (output, committed) = storage(&app, &id, move |store| {
        let sequence = store.last_seq;
        let result = store.mutate(&workspace, &principal.name, request)?;
        Ok((result, sequence != store.last_seq))
    })
    .await?;
    if committed {
        for record in &output.records {
            let name = if record.revision == 1 {
                "record:create"
            } else {
                "record:update"
            };
            let _ = app.events.send(json!({"name": name, "event": name, "collection": record.kind, "record": record_value(public_value(record))}));
        }
        for id in &output.deleted {
            let _ = app
                .events
                .send(json!({"name":"record:delete", "event":"record:delete", "id":id}));
        }
    }
    Ok(Json(serde_json::to_value(output).map_err(|_| {
        error(
            &id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Serialization failed",
        )
    })?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LeaseRequest {
    resource: String,
    owner: String,
    action: String,
    fence: Option<u64>,
    ttl_ms: Option<u64>,
}
pub(super) async fn lease(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    body: Result<Json<LeaseRequest>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let request = body.map_err(|r| body_error(&id, r))?.0;
    let output = storage(&app, &id, move |store| {
        store.change_lease(
            &workspace,
            request.resource,
            request.owner,
            &request.action,
            request.fence,
            request.ttl_ms,
        )
    })
    .await?;
    Ok(Json(json!({"lease": output})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LeaseQuery {
    resource: String,
}
pub(super) async fn lease_status(
    State(app): State<App>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestId>,
    session: Option<Extension<accounts::SessionIdentity>>,
    query: Result<Query<LeaseQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    backend_only(&id, &session)?;
    let workspace = server_workspace(&app);
    authorize(&principal, &workspace, &id)?;
    let resource = query
        .map_err(|_| {
            error(
                &id,
                StatusCode::BAD_REQUEST,
                "invalid_lease",
                "Invalid lease resource",
            )
        })?
        .0
        .resource;
    if resource.is_empty() || resource.len() > 512 || resource.contains('\0') {
        return Err(error(
            &id,
            StatusCode::BAD_REQUEST,
            "invalid_lease",
            "Invalid lease resource",
        ));
    }
    let output = storage(&app, &id, move |store| {
        Ok(store.resource_lease(&workspace, &resource))
    })
    .await?;
    let active = output
        .as_ref()
        .is_some_and(|l| l.expires_at > chrono::Utc::now().timestamp_millis());
    Ok(Json(json!({"lease": output, "active": active})))
}
