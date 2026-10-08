//! Durable constraints, bounded transactions and fenced resource ownership.
use super::*;

pub const MAX_MUTATION_OPERATIONS: usize = 1000;
pub const MAX_MUTATION_BYTES: usize = 8 * 1024 * 1024;
pub(super) fn key_digest(key: &str) -> String {
    hex(&Sha256::digest(key.as_bytes()))
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionConstraints {
    #[serde(default)]
    pub unique: Vec<String>,
    #[serde(default)]
    pub references: Vec<ReferenceConstraint>,
    pub lease: Option<LeasePolicy>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceConstraint {
    pub field: String,
    pub collection: String,
    pub target_field: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeasePolicy {
    pub field: String,
    pub prefix: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseProof {
    pub resource: String,
    pub owner: String,
    pub fence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub resource: String,
    pub owner: String,
    pub fence: u64,
    pub expires_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Create {
        collection: String,
        data: Map<String, Value>,
    },
    Update {
        id: String,
        data: Map<String, Value>,
        expected_revision: u64,
    },
    Delete {
        id: String,
        expected_revision: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRequest {
    pub idempotency_key: String,
    pub operations: Vec<Mutation>,
    #[serde(default)]
    pub leases: Vec<LeaseProof>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct MutationResult {
    pub records: Vec<Artifact>,
    pub deleted: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct MutationReceipt {
    fingerprint: String,
    result: MutationResult,
    #[serde(default)]
    pub(super) created_at_ms: i64,
}

fn fail(code: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, code)
}
fn scoped(workspace: &str, name: &str) -> String {
    format!("{workspace}\0{name}")
}
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && !name.contains('\0')
}
fn valid_field(name: &str) -> bool {
    valid_name(name)
        && ![
            "id",
            "type",
            "collection",
            "revision",
            "created_at",
            "updated_at",
            "expires_at",
            "ttl",
        ]
        .contains(&name)
}

impl Store {
    pub fn collection_constraints(
        &self,
        workspace: &str,
        collection: &str,
    ) -> Option<CollectionConstraints> {
        self.constraints
            .get(&scoped(workspace, collection))
            .cloned()
    }

    /// Installation is idempotent; replacing/removing a live policy is deliberately unsupported.
    pub fn configure_collection(
        &mut self,
        workspace: &str,
        collection: String,
        constraints: CollectionConstraints,
    ) -> io::Result<()> {
        if !valid_name(&collection)
            || constraints.unique.len() > 16
            || constraints.references.len() > 16
            || constraints.unique.iter().any(|f| !valid_field(f))
            || constraints.references.iter().any(|r| {
                !valid_field(&r.field)
                    || !valid_field(&r.target_field)
                    || !valid_name(&r.collection)
            })
            || constraints
                .lease
                .as_ref()
                .is_some_and(|l| !valid_field(&l.field) || !valid_name(&l.prefix))
        {
            return Err(fail("invalid_constraints"));
        }
        let key = scoped(workspace, &collection);
        if let Some(existing) = self.constraints.get(&key) {
            return if *existing == constraints {
                Ok(())
            } else {
                Err(fail("constraint_conflict"))
            };
        }
        if self.constraints.len() >= 1000 {
            return Err(fail("constraint_limit"));
        }
        if constraints.unique.iter().collect::<BTreeSet<_>>().len() != constraints.unique.len() {
            return Err(fail("invalid_constraints"));
        }
        for reference in &constraints.references {
            let target = if reference.collection == collection {
                Some(&constraints)
            } else {
                self.constraints
                    .get(&scoped(workspace, &reference.collection))
            };
            if !target.is_some_and(|c| c.unique.contains(&reference.target_field)) {
                return Err(fail("invalid_constraints"));
            }
        }
        let mut policies = self.constraints.clone();
        policies.insert(key, constraints.clone());
        self.validate_records(&self.artifacts, &policies)?;
        self.commit(Event::CollectionConfigured {
            workspace: workspace.into(),
            collection,
            constraints,
        })
    }

    fn validate_records(
        &self,
        records: &BTreeMap<String, Arc<Artifact>>,
        policies: &BTreeMap<String, CollectionConstraints>,
    ) -> io::Result<()> {
        let mut unique = BTreeSet::new();
        for record in records.values() {
            if let Some(policy) = policies.get(&scoped(&record.workspace_id, &record.kind)) {
                // TTL would permit parents to disappear outside reference/lease validation.
                if record.data.contains_key("ttl") || record.data.contains_key("expires_at") {
                    return Err(fail("constraint_ttl"));
                }
                for field in &policy.unique {
                    let value = record
                        .data
                        .get(field)
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty() && v.len() <= 256)
                        .ok_or_else(|| fail("invalid_constraint_value"))?;
                    if !unique.insert((
                        record.workspace_id.clone(),
                        record.kind.clone(),
                        field.clone(),
                        value.to_owned(),
                    )) {
                        return Err(fail("unique_conflict"));
                    }
                }
                if let Some(guard) = &policy.lease {
                    record
                        .data
                        .get(&guard.field)
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty() && v.len() <= 256)
                        .ok_or_else(|| fail("invalid_constraint_value"))?;
                }
            }
        }
        for record in records.values() {
            if let Some(policy) = policies.get(&scoped(&record.workspace_id, &record.kind)) {
                for reference in &policy.references {
                    let value = record
                        .data
                        .get(&reference.field)
                        .and_then(Value::as_str)
                        .ok_or_else(|| fail("invalid_constraint_value"))?;
                    if !unique.contains(&(
                        record.workspace_id.clone(),
                        reference.collection.clone(),
                        reference.target_field.clone(),
                        value.to_owned(),
                    )) {
                        return Err(fail("reference_conflict"));
                    }
                }
            }
        }
        Ok(())
    }

    fn require_lease(&self, record: &Artifact, proofs: &[LeaseProof]) -> io::Result<()> {
        if let Some(guard) = self
            .constraints
            .get(&scoped(&record.workspace_id, &record.kind))
            .and_then(|p| p.lease.as_ref())
        {
            let value = record
                .data
                .get(&guard.field)
                .and_then(Value::as_str)
                .ok_or_else(|| fail("invalid_constraint_value"))?;
            let resource = format!("{}{}", guard.prefix, value);
            let proof = proofs
                .iter()
                .find(|p| p.resource == resource)
                .ok_or_else(|| fail("lease_required"))?;
            let lease = self
                .leases
                .get(&scoped(&record.workspace_id, &resource))
                .ok_or_else(|| fail("lease_conflict"))?;
            if lease.owner != proof.owner
                || lease.fence != proof.fence
                || lease.expires_at <= Utc::now().timestamp_millis()
            {
                return Err(fail("lease_conflict"));
            }
        }
        Ok(())
    }

    /// Validate every record-producing event, including the legacy CRUD/bulk paths.
    pub(super) fn validate_mutation_event(&self, event: &Event) -> io::Result<()> {
        if self.constraints.is_empty()
            || matches!(
                event,
                Event::CollectionConfigured { .. }
                    | Event::LeaseChanged { .. }
                    | Event::MutationReceipt { .. }
                    | Event::MutationKeysExpired { .. }
                    | Event::LeaseRemoved { .. }
                    | Event::DeletionJobStarted { .. }
            )
        {
            return Ok(());
        }
        let mut staged = self.artifacts.clone();
        fn stage(
            store: &Store,
            staged: &mut BTreeMap<String, Arc<Artifact>>,
            event: &Event,
            proofs: &[LeaseProof],
        ) -> io::Result<()> {
            match event {
                Event::Batch { events } => {
                    for e in events {
                        stage(store, staged, e, proofs)?;
                    }
                }
                Event::MutationCommitted {
                    events,
                    proofs: supplied,
                    ..
                } => {
                    for e in events {
                        stage(store, staged, e, supplied)?;
                    }
                }
                Event::Artifact { artifact, .. } | Event::FileUploaded { artifact, .. } => {
                    if let Some(old) = staged.get(&artifact.id) {
                        store.require_lease(old, proofs)?;
                    }
                    store.check_not_blocked(artifact)?;
                    store.require_lease(artifact, proofs)?;
                    staged.insert(artifact.id.clone(), Arc::new(artifact.clone()));
                }
                Event::ArtifactDeleted { id } => {
                    if let Some(old) = staged.get(id) {
                        store.check_not_blocked(old)?;
                        store.require_lease(old, proofs)?;
                    }
                    staged.remove(id);
                }
                Event::DeletionJobProgress {
                    job_id,
                    ids,
                    completed,
                } => {
                    store.check_deletion_progress(job_id, ids, *completed)?;
                    for id in ids {
                        staged.remove(id);
                    }
                }
                Event::FileDeleted { id } => {
                    if let Some(file) = store.files.get(id) {
                        if let Some(old) = staged.get(&file.artifact_id) {
                            store.require_lease(old, proofs)?;
                        }
                        staged.remove(&file.artifact_id);
                    }
                }
                _ => {}
            }
            Ok(())
        }
        stage(self, &mut staged, event, &[])?;
        self.validate_records(&staged, &self.constraints)
    }

    pub fn mutate(
        &mut self,
        workspace: &str,
        scope: &str,
        request: MutationRequest,
    ) -> io::Result<MutationResult> {
        if request.idempotency_key.is_empty()
            || request.idempotency_key.len() > 128
            || request.idempotency_key.contains('\0')
            || request.operations.is_empty()
            || request.operations.len() > MAX_MUTATION_OPERATIONS
            || request.leases.len() > MAX_MUTATION_OPERATIONS
        {
            return Err(fail("invalid_mutation"));
        }
        let encoded = serde_json::to_vec(&request.operations).map_err(io::Error::other)?;
        if serde_json::to_vec(&request)
            .map_err(io::Error::other)?
            .len()
            > MAX_MUTATION_BYTES
        {
            return Err(fail("mutation_too_large"));
        }
        // Proofs can change on retry after lease renewal/reacquisition; the mutation must not.
        let fingerprint = hex(&Sha256::digest(encoded));
        let key = serde_json::to_string(&(workspace, scope, &request.idempotency_key))
            .map_err(io::Error::other)?;
        if let Some(receipt) = self.mutation_receipts.get(&key) {
            return if receipt.fingerprint == fingerprint {
                Ok(receipt.result.clone())
            } else {
                Err(fail("mutation_conflict"))
            };
        }
        if self.spent_mutation_keys.contains(&key_digest(&key)) {
            return Err(fail("mutation_expired"));
        }
        let receipt_cutoff = Utc::now().timestamp_millis() - self.receipt_retention_ms;
        let expired_keys: Vec<String> = if self.mutation_receipts.len() >= self.receipt_limit {
            self.mutation_receipts
                .iter()
                .filter(|(_, receipt)| {
                    receipt.created_at_ms != 0 && receipt.created_at_ms <= receipt_cutoff
                })
                .map(|(key, _)| key.clone())
                .collect()
        } else {
            Vec::new()
        };
        if self
            .mutation_receipts
            .len()
            .saturating_sub(expired_keys.len())
            >= self.receipt_limit
        {
            return Err(fail("receipt_limit"));
        }
        let mut staged = self.artifacts.clone();
        let mut events = Vec::new();
        let mut result = MutationResult {
            records: Vec::new(),
            deleted: Vec::new(),
        };
        let mut touched = BTreeSet::new();
        let mut staged_bytes = 0usize;
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        for op in request.operations {
            match op {
                Mutation::Create { collection, data } => {
                    if !valid_name(&collection) {
                        return Err(fail("invalid_mutation"));
                    }
                    if serde_json::to_vec(&data).map_err(io::Error::other)?.len() > MAX_RECORD_BYTES
                    {
                        return Err(fail("mutation_too_large"));
                    }
                    if data.contains_key("ttl") || data.contains_key("expires_at") {
                        return Err(fail("constraint_ttl"));
                    }
                    let artifact = Artifact {
                        id: new_id("art"),
                        workspace_id: workspace.into(),
                        kind: collection,
                        data,
                        revision: 1,
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    };
                    staged_bytes += serde_json::to_vec(&artifact)
                        .map_err(io::Error::other)?
                        .len();
                    if staged_bytes > MAX_MUTATION_BYTES {
                        return Err(fail("mutation_too_large"));
                    }
                    staged.insert(artifact.id.clone(), Arc::new(artifact.clone()));
                    events.push(Event::Artifact {
                        artifact: artifact.clone(),
                        idempotency: None,
                        fingerprint: String::new(),
                    });
                    result.records.push(artifact);
                }
                Mutation::Update {
                    id,
                    data,
                    expected_revision,
                } => {
                    if !touched.insert(id.clone()) {
                        return Err(fail("invalid_mutation"));
                    }
                    let current = staged
                        .get(&id)
                        .filter(|a| a.workspace_id == workspace && !Self::is_expired(a, &now))
                        .ok_or_else(|| fail("mutation_not_found"))?;
                    if current.revision != expected_revision {
                        return Err(fail("revision_conflict"));
                    }
                    let mut artifact = current.as_ref().clone();
                    artifact.data.extend(data);
                    if artifact.data.contains_key("ttl") || artifact.data.contains_key("expires_at")
                    {
                        return Err(fail("constraint_ttl"));
                    }
                    if serde_json::to_vec(&artifact.data)
                        .map_err(io::Error::other)?
                        .len()
                        > MAX_RECORD_BYTES
                    {
                        return Err(fail("mutation_too_large"));
                    }
                    artifact.revision = artifact
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| fail("revision_conflict"))?;
                    artifact.updated_at = now.clone();
                    staged_bytes += serde_json::to_vec(&artifact)
                        .map_err(io::Error::other)?
                        .len();
                    if staged_bytes > MAX_MUTATION_BYTES {
                        return Err(fail("mutation_too_large"));
                    }
                    staged.insert(id, Arc::new(artifact.clone()));
                    events.push(Event::Artifact {
                        artifact: artifact.clone(),
                        idempotency: None,
                        fingerprint: String::new(),
                    });
                    result.records.push(artifact);
                }
                Mutation::Delete {
                    id,
                    expected_revision,
                } => {
                    if !touched.insert(id.clone()) {
                        return Err(fail("invalid_mutation"));
                    }
                    let current = staged
                        .get(&id)
                        .filter(|a| a.workspace_id == workspace && !Self::is_expired(a, &now))
                        .ok_or_else(|| fail("mutation_not_found"))?;
                    if current.revision != expected_revision {
                        return Err(fail("revision_conflict"));
                    }
                    if self.files.values().any(|f| f.artifact_id == id) {
                        return Err(fail("invalid_mutation"));
                    }
                    staged.remove(&id);
                    events.push(Event::ArtifactDeleted { id: id.clone() });
                    result.deleted.push(id);
                }
            }
        }
        if serde_json::to_vec(&result).map_err(io::Error::other)?.len() > MAX_MUTATION_BYTES {
            return Err(fail("mutation_too_large"));
        }
        let receipt = MutationReceipt {
            fingerprint,
            result: result.clone(),
            created_at_ms: Utc::now().timestamp_millis(),
        };
        let mutation = Event::MutationCommitted {
            key,
            receipt,
            events,
            proofs: request.leases,
        };
        if expired_keys.is_empty() {
            self.commit(mutation)?;
        } else {
            let digests = expired_keys.iter().map(|key| key_digest(key)).collect();
            self.commit(Event::Batch {
                events: vec![Event::MutationKeysExpired { keys: digests }, mutation],
            })?;
            for key in expired_keys {
                self.mutation_receipts.remove(&key);
            }
        }
        // Derived files are best effort; journal replay repairs them after a crash.
        for record in &result.records {
            let _ = self.reproject(record);
        }
        for id in &result.deleted {
            let _ = fs::remove_file(self.root.join("artifacts").join(format!("{id}.toon")));
        }
        Ok(result)
    }

    pub fn persistence_status(&self) -> Value {
        let now = Utc::now().timestamp_millis();
        serde_json::json!({
            "active_receipts": self.mutation_receipts.len(),
            "receipt_limit": self.receipt_limit,
            "receipt_retention_days": self.receipt_retention_ms / 86_400_000,
            "spent_keys": self.spent_mutation_keys.len(),
            "active_leases": self.leases.values().filter(|lease| lease.expires_at > now).count(),
            "active_lease_limit": self.active_lease_limit,
            "pending_deletion_jobs": self.deletion_jobs.values().filter(|job| !job.completed).count(),
            "indexed_keys": self.field_index.len(),
        })
    }

    pub fn resource_lease(&self, workspace: &str, resource: &str) -> Option<Lease> {
        self.leases.get(&scoped(workspace, resource)).cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn change_lease(
        &mut self,
        workspace: &str,
        resource: String,
        owner: String,
        action: &str,
        fence: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> io::Result<Lease> {
        if resource.is_empty()
            || resource.len() > 512
            || resource.contains('\0')
            || !valid_name(&owner)
        {
            return Err(fail("invalid_lease"));
        }
        let key = scoped(workspace, &resource);
        if action == "acquire" && self.blocked_resources.contains(&key) {
            return Err(fail("deletion_in_progress"));
        }
        let now = Utc::now().timestamp_millis();
        let previous = self.leases.get(&key);
        let ttl = ttl_ms.unwrap_or(30_000);
        if action != "release" && !(1000..=300_000).contains(&ttl) {
            return Err(fail("invalid_lease"));
        }
        let active = previous.filter(|l| l.expires_at > now);
        let next = match action {
            "acquire" => {
                if let Some(active) = active {
                    // Same-owner retries are safe, but do not extend lifetime.
                    if active.owner == owner {
                        return Ok(active.clone());
                    }
                    return Err(fail("lease_conflict"));
                }
                Lease {
                    resource,
                    owner,
                    fence: self
                        .last_seq
                        .checked_add(1)
                        .ok_or_else(|| fail("lease_conflict"))?,
                    expires_at: now + ttl as i64,
                }
            }
            "renew" | "release" => {
                let previous = previous.ok_or_else(|| fail("lease_conflict"))?;
                if previous.owner != owner
                    || Some(previous.fence) != fence
                    || (action == "renew" && active.is_none())
                {
                    return Err(fail("lease_conflict"));
                }
                Lease {
                    expires_at: if action == "release" {
                        0
                    } else {
                        now + ttl as i64
                    },
                    ..previous.clone()
                }
            }
            _ => return Err(fail("invalid_lease")),
        };
        if action == "release" {
            self.commit(Event::LeaseRemoved { key })?;
        } else if previous.is_none() && self.leases.len() >= self.active_lease_limit {
            let expired: Vec<String> = self
                .leases
                .iter()
                .filter(|(_, lease)| lease.expires_at <= now)
                .take(1000)
                .map(|(key, _)| key.clone())
                .collect();
            if expired.is_empty() {
                return Err(fail("lease_limit"));
            }
            let mut events: Vec<Event> = expired
                .into_iter()
                .map(|key| Event::LeaseRemoved { key })
                .collect();
            events.push(Event::LeaseChanged {
                key,
                lease: next.clone(),
            });
            self.commit(Event::Batch { events })?;
        } else {
            self.commit(Event::LeaseChanged {
                key,
                lease: next.clone(),
            })?;
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;
    use std::sync::{Arc, Barrier, Mutex};

    fn data(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }
    fn policy() -> CollectionConstraints {
        CollectionConstraints {
            unique: vec!["appId".into()],
            ..Default::default()
        }
    }
    fn request(key: &str, operations: Vec<Mutation>) -> MutationRequest {
        MutationRequest {
            idempotency_key: key.into(),
            operations,
            leases: vec![],
        }
    }
    fn create(collection: &str, value: Value) -> Mutation {
        Mutation::Create {
            collection: collection.into(),
            data: data(value),
        }
    }
    fn setup(store: &mut Store) {
        store
            .configure_collection("a", "parents".into(), policy())
            .unwrap();
        store
            .configure_collection(
                "a",
                "children".into(),
                CollectionConstraints {
                    references: vec![ReferenceConstraint {
                        field: "parentId".into(),
                        collection: "parents".into(),
                        target_field: "appId".into(),
                    }],
                    ..policy()
                },
            )
            .unwrap();
    }

    #[test]
    fn atomic_mutations_validate_final_state_and_all_legacy_paths() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        setup(&mut store);
        let before = store.last_seq;
        // Child before parent is valid when both exist in the committed state.
        let result = store
            .mutate(
                "a",
                "backend",
                request(
                    "seed",
                    vec![
                        create("children", json!({"appId":"c","parentId":"p"})),
                        create("parents", json!({"appId":"p"})),
                    ],
                ),
            )
            .unwrap();
        assert_eq!(store.last_seq, before + 1);
        let child = &result.records[0];
        let parent = &result.records[1];
        let checkpoint = store.last_seq;
        let error = store
            .mutate(
                "a",
                "backend",
                request(
                    "invalid",
                    vec![
                        create("parents", json!({"appId":"new"})),
                        create("children", json!({"appId":"broken","parentId":"missing"})),
                    ],
                ),
            )
            .err()
            .unwrap();
        assert_eq!(error.to_string(), "reference_conflict");
        assert_eq!(store.last_seq, checkpoint);
        assert_eq!(store.list("a", None).len(), 2);
        assert_eq!(
            store
                .create("a", "parents".into(), data(json!({"appId":"p"})), None)
                .err()
                .unwrap()
                .to_string(),
            "unique_conflict"
        );
        assert!(
            store
                .bulk_insert(
                    "a",
                    Some("parents".into()),
                    vec![
                        (None, data(json!({"appId":"x"}))),
                        (None, data(json!({"appId":"x"})))
                    ]
                )
                .is_err()
        );
        assert!(
            store
                .update(
                    "a",
                    &child.id,
                    data(json!({"parentId":"missing"})),
                    true,
                    None
                )
                .is_err()
        );
        assert!(store.delete("a", &parent.id, None).is_err());
        assert!(
            store
                .bulk_delete("a", vec![parent.id.clone()], None)
                .is_err()
        );
        assert_eq!(store.last_seq, checkpoint);
        // Parent deletion may precede child deletion in the batch.
        store
            .mutate(
                "a",
                "backend",
                request(
                    "delete",
                    vec![
                        Mutation::Delete {
                            id: parent.id.clone(),
                            expected_revision: 1,
                        },
                        Mutation::Delete {
                            id: child.id.clone(),
                            expected_revision: 1,
                        },
                    ],
                ),
            )
            .unwrap();
        assert!(store.list("a", None).is_empty());
    }

    #[test]
    fn receipts_constraints_and_revisions_survive_restart_and_vacuum() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .configure_collection("a", "parents".into(), policy())
            .unwrap();
        let original = request("create", vec![create("parents", json!({"appId":"p"}))]);
        let result = store.mutate("a", "backend", original.clone()).unwrap();
        let id = result.records[0].id.clone();
        store
            .mutate(
                "a",
                "backend",
                request(
                    "update",
                    vec![Mutation::Update {
                        id: id.clone(),
                        expected_revision: 1,
                        data: data(json!({"name":"edited"})),
                    }],
                ),
            )
            .unwrap();
        let seq = store.last_seq;
        assert_eq!(
            store
                .mutate(
                    "a",
                    "backend",
                    request(
                        "stale",
                        vec![Mutation::Update {
                            id: id.clone(),
                            expected_revision: 1,
                            data: Map::new()
                        }]
                    )
                )
                .err()
                .unwrap()
                .to_string(),
            "revision_conflict"
        );
        assert_eq!(store.last_seq, seq);
        assert!(
            store
                .mutate(
                    "a",
                    "backend",
                    request("create", vec![create("parents", json!({"appId":"other"}))])
                )
                .is_err()
        );
        store
            .mutate(
                "a",
                "backend",
                request(
                    "delete",
                    vec![Mutation::Delete {
                        id: id.clone(),
                        expected_revision: 2,
                    }],
                ),
            )
            .unwrap();
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        let seq = store.last_seq;
        // A lost-response retry returns the historical receipt, never recreates deleted data.
        assert_eq!(
            store.mutate("a", "backend", original).unwrap().records[0].id,
            id
        );
        assert_eq!(store.last_seq, seq);
        assert!(store.list("a", None).is_empty());
        assert_eq!(store.collection_constraints("a", "parents"), Some(policy()));
        assert!(
            store
                .configure_collection("a", "parents".into(), CollectionConstraints::default())
                .is_err()
        );
        store
            .configure_collection("a", "parents".into(), policy())
            .unwrap();
        assert_eq!(store.last_seq, seq);
    }

    #[test]
    fn fencing_rejects_expired_owners_and_guard_changes_on_every_write_path() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .configure_collection(
                "a",
                "messages".into(),
                CollectionConstraints {
                    lease: Some(LeasePolicy {
                        field: "conversationId".into(),
                        prefix: "conversation:".into(),
                    }),
                    ..policy()
                },
            )
            .unwrap();
        let old = store
            .change_lease(
                "a",
                "conversation:c".into(),
                "run-1".into(),
                "acquire",
                None,
                Some(1000),
            )
            .unwrap();
        assert!(
            store
                .change_lease(
                    "a",
                    "conversation:c".into(),
                    "run-2".into(),
                    "acquire",
                    None,
                    None
                )
                .is_err()
        );
        let retried = store
            .change_lease(
                "a",
                "conversation:c".into(),
                "run-1".into(),
                "acquire",
                None,
                None,
            )
            .unwrap();
        assert_eq!(retried.expires_at, old.expires_at);
        let mut turn = request(
            "turn",
            vec![create(
                "messages",
                json!({"appId":"m","conversationId":"c"}),
            )],
        );
        assert_eq!(
            store
                .mutate("a", "backend", turn.clone())
                .err()
                .unwrap()
                .to_string(),
            "lease_required"
        );
        let expired = Lease {
            expires_at: Utc::now().timestamp_millis() - 1,
            ..old.clone()
        };
        store
            .commit(Event::LeaseChanged {
                key: scoped("a", "conversation:c"),
                lease: expired,
            })
            .unwrap();
        let proof = LeaseProof {
            resource: old.resource.clone(),
            owner: old.owner.clone(),
            fence: old.fence,
        };
        turn.leases = vec![proof.clone()];
        assert_eq!(
            store
                .mutate("a", "backend", turn.clone())
                .err()
                .unwrap()
                .to_string(),
            "lease_conflict"
        );
        assert!(
            store
                .change_lease(
                    "a",
                    old.resource.clone(),
                    old.owner.clone(),
                    "renew",
                    Some(old.fence),
                    None
                )
                .is_err()
        );
        let next = store
            .change_lease(
                "a",
                old.resource.clone(),
                "run-2".into(),
                "acquire",
                None,
                None,
            )
            .unwrap();
        assert!(next.fence > old.fence);
        assert!(
            store
                .change_lease(
                    "a",
                    old.resource.clone(),
                    old.owner.clone(),
                    "release",
                    Some(old.fence),
                    None
                )
                .is_err()
        );
        turn.leases = vec![LeaseProof {
            resource: next.resource.clone(),
            owner: next.owner.clone(),
            fence: next.fence,
        }];
        let record = store
            .mutate("a", "backend", turn.clone())
            .unwrap()
            .records
            .remove(0);
        assert!(
            store
                .create(
                    "a",
                    "messages".into(),
                    data(json!({"appId":"bypass","conversationId":"c"})),
                    None
                )
                .is_err()
        );
        assert!(
            store
                .update(
                    "a",
                    &record.id,
                    data(json!({"content":"bypass"})),
                    true,
                    None
                )
                .is_err()
        );
        assert!(store.delete("a", &record.id, None).is_err());
        let mut moved = request(
            "move",
            vec![Mutation::Update {
                id: record.id.clone(),
                expected_revision: 1,
                data: data(json!({"conversationId":"different"})),
            }],
        );
        moved.leases = turn.leases.clone();
        assert_eq!(
            store
                .mutate("a", "backend", moved)
                .err()
                .unwrap()
                .to_string(),
            "lease_required"
        );
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(
            store.resource_lease("a", &next.resource).unwrap().fence,
            next.fence
        );
        store
            .change_lease(
                "a",
                next.resource.clone(),
                next.owner.clone(),
                "release",
                Some(next.fence),
                None,
            )
            .unwrap();
        assert!(
            store
                .mutate(
                    "a",
                    "backend",
                    request(
                        "stale-after-release",
                        vec![create(
                            "messages",
                            json!({"appId":"new","conversationId":"c"})
                        )]
                    )
                )
                .is_err()
        );
        let latest = store
            .change_lease("a", next.resource, "run-3".into(), "acquire", None, None)
            .unwrap();
        assert!(latest.fence > next.fence);
        // Receipt replay needs no current lease and does not write again.
        assert_eq!(
            store.mutate("a", "backend", turn).unwrap().records[0].id,
            record.id
        );
    }

    #[test]
    fn simultaneous_duplicate_creates_admit_one_application_identity() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .configure_collection("a", "parents".into(), policy())
            .unwrap();
        let store = Arc::new(Mutex::new(store));
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .lock()
                        .unwrap()
                        .mutate(
                            "a",
                            "backend",
                            request(
                                &format!("concurrent-{i}"),
                                vec![create("parents", json!({"appId":"same"}))],
                            ),
                        )
                        .is_ok()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|h| usize::from(h.join().unwrap()))
                .sum::<usize>(),
            1
        );
        assert_eq!(store.lock().unwrap().list("a", Some("parents")).len(), 1);
    }

    #[test]
    fn oversized_merged_result_and_invalid_policy_leave_journal_unchanged() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        // A small update request must not produce an unbounded merged receipt/result.
        let large = store
            .create(
                "a",
                "legacy".into(),
                data(json!({"content":"x".repeat(MAX_MUTATION_BYTES)})),
                None,
            )
            .unwrap();
        let sequence = store.last_seq;
        let error = store
            .mutate(
                "a",
                "backend",
                request(
                    "large-result",
                    vec![Mutation::Update {
                        id: large.id.clone(),
                        expected_revision: 1,
                        data: data(json!({"title":"small request"})),
                    }],
                ),
            )
            .err()
            .unwrap();
        assert_eq!(error.to_string(), "mutation_too_large");
        assert_eq!(store.last_seq, sequence);
        assert_eq!(store.get("a", &large.id).unwrap().revision, 1);
        assert!(
            store
                .configure_collection("a", "legacy".into(), policy())
                .is_err()
        );
        assert_eq!(store.last_seq, sequence);
        assert!(store.collection_constraints("a", "legacy").is_none());
        let invalid = CollectionConstraints {
            references: vec![ReferenceConstraint {
                field: "parentId".into(),
                collection: "unconfigured".into(),
                target_field: "appId".into(),
            }],
            ..Default::default()
        };
        assert!(
            store
                .configure_collection("a", "children".into(), invalid)
                .is_err()
        );
        assert_eq!(store.last_seq, sequence);
    }

    #[test]
    fn receipt_expiry_prevents_replay_without_filling_active_capacity() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.receipt_limit = 1;
        store.receipt_retention_ms = 60_000;
        store
            .configure_collection("a", "parents".into(), policy())
            .unwrap();
        let original = request("one", vec![create("parents", json!({"appId":"one"}))]);
        store.mutate("a", "backend", original.clone()).unwrap();
        assert_eq!(
            store
                .mutate(
                    "a",
                    "backend",
                    request("two", vec![create("parents", json!({"appId":"two"}))])
                )
                .err()
                .unwrap()
                .to_string(),
            "receipt_limit"
        );
        let receipt = store.mutation_receipts.values_mut().next().unwrap();
        receipt.created_at_ms = Utc::now().timestamp_millis() - 1000;
        store.receipt_retention_ms = 1;
        store
            .mutate(
                "a",
                "backend",
                request("two", vec![create("parents", json!({"appId":"two"}))]),
            )
            .unwrap();
        assert_eq!(
            store
                .mutate("a", "backend", original.clone())
                .err()
                .unwrap()
                .to_string(),
            "mutation_expired"
        );
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(
            store
                .mutate("a", "backend", original)
                .err()
                .unwrap()
                .to_string(),
            "mutation_expired"
        );
        assert_eq!(store.list("a", Some("parents")).len(), 2);
    }

    #[test]
    fn lease_capacity_applies_only_to_active_owners_and_fences_never_repeat() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.active_lease_limit = 1;
        let first = store
            .change_lease("a", "r1".into(), "one".into(), "acquire", None, None)
            .unwrap();
        assert_eq!(
            store
                .change_lease("a", "r2".into(), "two".into(), "acquire", None, None)
                .err()
                .unwrap()
                .to_string(),
            "lease_limit"
        );
        store
            .change_lease(
                "a",
                "r1".into(),
                "one".into(),
                "release",
                Some(first.fence),
                None,
            )
            .unwrap();
        assert!(store.resource_lease("a", "r1").is_none());
        let second = store
            .change_lease("a", "r2".into(), "two".into(), "acquire", None, None)
            .unwrap();
        assert!(second.fence > first.fence);
        let expired = Lease {
            expires_at: Utc::now().timestamp_millis() - 1,
            ..second.clone()
        };
        store
            .commit(Event::LeaseChanged {
                key: scoped("a", "r2"),
                lease: expired,
            })
            .unwrap();
        let third = store
            .change_lease("a", "r3".into(), "three".into(), "acquire", None, None)
            .unwrap();
        assert!(third.fence > second.fence);
        assert!(store.resource_lease("a", "r2").is_none());
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        store.active_lease_limit = 1;
        let active = store.resource_lease("a", "r3").unwrap();
        assert_eq!(active.fence, third.fence);
        store
            .change_lease(
                "a",
                "r3".into(),
                "three".into(),
                "release",
                Some(third.fence),
                None,
            )
            .unwrap();
        let fourth = store
            .change_lease("a", "r1".into(), "four".into(), "acquire", None, None)
            .unwrap();
        assert!(fourth.fence > third.fence);
        assert_ne!(fourth.fence, first.fence);
    }

    #[test]
    fn incomplete_transaction_frame_recovers_without_partial_effects_or_receipt() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        setup(&mut store);
        let prefix = fs::read(directory.0.join("journal.jsonl")).unwrap();
        let head = fs::read(directory.0.join("ledger-head.json")).unwrap();
        let transaction = request(
            "batch",
            vec![
                create("parents", json!({"appId":"p"})),
                create("children", json!({"appId":"c","parentId":"p"})),
            ],
        );
        let first = store.mutate("a", "backend", transaction.clone()).unwrap();
        let committed = fs::read(directory.0.join("journal.jsonl")).unwrap();
        drop(store);
        let mut recovered = Store::open(&directory.0).unwrap();
        assert_eq!(
            recovered
                .mutate("a", "backend", transaction.clone())
                .unwrap()
                .records[0]
                .id,
            first.records[0].id
        );
        drop(recovered);
        fs::write(
            directory.0.join("journal.jsonl"),
            &committed[..prefix.len() + (committed.len() - prefix.len()) / 2],
        )
        .unwrap();
        fs::write(directory.0.join("ledger-head.json"), head).unwrap();
        let mut recovered = Store::open(&directory.0).unwrap();
        assert!(recovered.list("a", None).is_empty());
        let fresh = recovered.mutate("a", "backend", transaction).unwrap();
        assert_ne!(fresh.records[0].id, first.records[0].id);
        assert_eq!(recovered.list("a", None).len(), 2);
    }
}
