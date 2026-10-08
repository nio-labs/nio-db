//! Durable, restartable cascade deletion of records linked by installed references.
use super::mutations::MAX_MUTATION_BYTES;
use super::*;

pub const MAX_DELETION_JOB_RECORDS: usize = 100_000;
pub const DELETION_BATCH_SIZE: usize = 1000;

#[derive(Clone, Serialize, Deserialize)]
pub struct DeletionJob {
    pub id: String,
    pub workspace: String,
    pub scope: String,
    pub idempotency_key: String,
    pub root_id: String,
    pub created_at_ms: i64,
    pub total: usize,
    pub cursor: usize,
    pub completed: bool,
    pub plan: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct DeletionJobStatus {
    pub id: String,
    pub root_id: String,
    pub status: &'static str,
    pub deleted: usize,
    pub total: usize,
    pub created_at_ms: i64,
}
impl From<&DeletionJob> for DeletionJobStatus {
    fn from(job: &DeletionJob) -> Self {
        Self {
            id: job.id.clone(),
            root_id: job.root_id.clone(),
            status: if job.completed { "complete" } else { "pending" },
            deleted: job.cursor,
            total: job.total,
            created_at_ms: job.created_at_ms,
        }
    }
}

impl Store {
    pub fn deletion_job(&self, workspace: &str, id: &str) -> Option<DeletionJobStatus> {
        self.deletion_jobs
            .get(id)
            .filter(|job| job.workspace == workspace)
            .map(Into::into)
    }
    pub fn pending_deletion_jobs(&self) -> Vec<String> {
        self.deletion_jobs
            .values()
            .filter(|job| !job.completed)
            .map(|job| job.id.clone())
            .collect()
    }

    pub(super) fn children_of(&self, parent: &Artifact) -> Vec<String> {
        let mut ids = BTreeSet::new();
        for (key, policy) in &self.constraints {
            let Some((workspace, collection)) = key.split_once('\0') else {
                continue;
            };
            if workspace != parent.workspace_id {
                continue;
            }
            for reference in &policy.references {
                if reference.collection != parent.kind {
                    continue;
                }
                let Some(value) = parent
                    .data
                    .get(&reference.target_field)
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let field_key = (
                    workspace.to_owned(),
                    collection.to_owned(),
                    reference.field.clone(),
                    value.to_owned(),
                );
                if let Some(matches) = self.field_index.get(&field_key) {
                    ids.extend(matches.iter().cloned());
                }
            }
        }
        ids.into_iter().collect()
    }

    fn deletion_plan(&self, root_id: &str) -> io::Result<Vec<String>> {
        let mut ordered = Vec::new();
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut stack = vec![(root_id.to_owned(), false, 0usize)];
        while let Some((id, exit, depth)) = stack.pop() {
            if exit {
                visiting.remove(&id);
                if visited.insert(id.clone()) {
                    ordered.push(id);
                }
                continue;
            }
            if visited.contains(&id) {
                continue;
            }
            if !visiting.insert(id.clone()) {
                return Err(io::Error::other("deletion_cycle"));
            }
            if depth > 32 || visiting.len() > MAX_DELETION_JOB_RECORDS {
                return Err(io::Error::other("deletion_job_too_large"));
            }
            let record = self
                .artifacts
                .get(&id)
                .ok_or_else(|| io::Error::other("deletion_job_conflict"))?;
            if self.files.values().any(|file| file.artifact_id == id) {
                return Err(io::Error::other("deletion_job_file"));
            }
            stack.push((id, true, depth));
            for child in self.children_of(record).into_iter().rev() {
                if visiting.contains(&child) {
                    return Err(io::Error::other("deletion_cycle"));
                }
                if !visited.contains(&child) {
                    stack.push((child, false, depth + 1));
                }
            }
        }
        if ordered.len() > MAX_DELETION_JOB_RECORDS {
            return Err(io::Error::other("deletion_job_too_large"));
        }
        Ok(ordered)
    }

    pub(super) fn rebuild_blocked_records(&mut self) {
        self.blocked_records.clear();
        self.blocked_resources.clear();
        for job in self.deletion_jobs.values().filter(|job| !job.completed) {
            for id in job.plan.iter().skip(job.cursor) {
                let Some(record) = self.artifacts.get(id) else {
                    continue;
                };
                self.blocked_records.insert(id.clone());
                if let Some(policy) = self
                    .constraints
                    .get(&format!("{}\0{}", record.workspace_id, record.kind))
                    && let Some(guard) = &policy.lease
                    && let Some(value) = record.data.get(&guard.field).and_then(Value::as_str)
                {
                    self.blocked_resources.insert(format!(
                        "{}\0{}{}",
                        record.workspace_id, guard.prefix, value
                    ));
                }
            }
        }
    }

    pub fn start_deletion_job(
        &mut self,
        workspace: &str,
        scope: &str,
        idempotency_key: &str,
        root_id: &str,
    ) -> io::Result<DeletionJobStatus> {
        if idempotency_key.is_empty()
            || idempotency_key.len() > 128
            || idempotency_key.contains('\0')
        {
            return Err(io::Error::other("invalid_deletion_job"));
        }
        if let Some(job) = self.deletion_jobs.values().find(|job| {
            job.workspace == workspace
                && job.scope == scope
                && job.idempotency_key == idempotency_key
        }) {
            return if job.root_id == root_id {
                Ok(job.into())
            } else {
                Err(io::Error::other("deletion_job_conflict"))
            };
        }
        if self.pending_deletion_jobs().len() >= 32 {
            return Err(io::Error::other("deletion_job_limit"));
        }
        let root = self
            .get_visible(workspace, root_id, None)
            .ok_or_else(|| io::Error::other("deletion_job_not_found"))?;
        if !self
            .constraints
            .contains_key(&format!("{workspace}\0{}", root.kind))
        {
            return Err(io::Error::other("invalid_deletion_job"));
        }
        let plan = self.deletion_plan(root_id)?;
        for id in &plan {
            let record = self.artifacts.get(id).unwrap();
            if let Some(policy) = self
                .constraints
                .get(&format!("{}\0{}", record.workspace_id, record.kind))
                && let Some(guard) = &policy.lease
                && let Some(value) = record.data.get(&guard.field).and_then(Value::as_str)
                && let Some(lease) = self
                    .leases
                    .get(&format!("{workspace}\0{}{}", guard.prefix, value))
                && lease.expires_at > Utc::now().timestamp_millis()
            {
                return Err(io::Error::other("lease_conflict"));
            }
        }
        let job = DeletionJob {
            id: new_id("del"),
            workspace: workspace.into(),
            scope: scope.into(),
            idempotency_key: idempotency_key.into(),
            root_id: root_id.into(),
            created_at_ms: Utc::now().timestamp_millis(),
            total: plan.len(),
            cursor: 0,
            completed: false,
            plan,
        };
        if serde_json::to_vec(&job).map_err(io::Error::other)?.len() > MAX_MUTATION_BYTES {
            return Err(io::Error::other("deletion_job_too_large"));
        }
        self.commit(Event::DeletionJobStarted { job: job.clone() })?;
        Ok((&job).into())
    }

    pub fn advance_deletion_job(
        &mut self,
        workspace: &str,
        id: &str,
    ) -> io::Result<DeletionJobStatus> {
        let job = self
            .deletion_jobs
            .get(id)
            .filter(|job| job.workspace == workspace)
            .ok_or_else(|| io::Error::other("deletion_job_not_found"))?
            .clone();
        if job.completed {
            return Ok((&job).into());
        }
        let end = (job.cursor + DELETION_BATCH_SIZE).min(job.total);
        let ids = job.plan[job.cursor..end].to_vec();
        let completed = end == job.total;
        self.commit(Event::DeletionJobProgress {
            job_id: job.id.clone(),
            ids: ids.clone(),
            completed,
        })?;
        for id in ids {
            let _ = fs::remove_file(self.root.join("artifacts").join(format!("{id}.toon")));
        }
        Ok(self.deletion_job(workspace, &job.id).unwrap())
    }

    pub(super) fn check_not_blocked(&self, artifact: &Artifact) -> io::Result<()> {
        if self.blocked_records.contains(&artifact.id) {
            return Err(io::Error::other("deletion_in_progress"));
        }
        if let Some(policy) = self
            .constraints
            .get(&format!("{}\0{}", artifact.workspace_id, artifact.kind))
        {
            for reference in &policy.references {
                let Some(value) = artifact.data.get(&reference.field).and_then(Value::as_str)
                else {
                    continue;
                };
                let index_key = (
                    artifact.workspace_id.clone(),
                    reference.collection.clone(),
                    reference.target_field.clone(),
                    value.to_owned(),
                );
                if self
                    .field_index
                    .get(&index_key)
                    .is_some_and(|ids| ids.iter().any(|id| self.blocked_records.contains(id)))
                {
                    return Err(io::Error::other("deletion_in_progress"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_deletion_progress(
        &self,
        job_id: &str,
        ids: &[String],
        completed: bool,
    ) -> io::Result<()> {
        let job = self
            .deletion_jobs
            .get(job_id)
            .filter(|job| !job.completed)
            .ok_or_else(|| io::Error::other("deletion_job_conflict"))?;
        let end = job
            .cursor
            .checked_add(ids.len())
            .ok_or_else(|| io::Error::other("deletion_job_conflict"))?;
        if ids.is_empty()
            || end > job.total
            || job.plan[job.cursor..end] != *ids
            || completed != (end == job.total)
        {
            return Err(io::Error::other("deletion_job_conflict"));
        }
        for id in ids {
            let record = self
                .artifacts
                .get(id)
                .ok_or_else(|| io::Error::other("deletion_job_conflict"))?;
            if !self.blocked_records.contains(id) {
                return Err(io::Error::other("deletion_job_conflict"));
            }
            if let Some(policy) = self
                .constraints
                .get(&format!("{}\0{}", record.workspace_id, record.kind))
                && let Some(guard) = &policy.lease
                && let Some(value) = record.data.get(&guard.field).and_then(Value::as_str)
                && let Some(lease) = self.leases.get(&format!(
                    "{}\0{}{}",
                    record.workspace_id, guard.prefix, value
                ))
                && lease.expires_at > Utc::now().timestamp_millis()
            {
                return Err(io::Error::other("lease_conflict"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;

    fn data(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }
    fn policy(
        unique: bool,
        references: Vec<ReferenceConstraint>,
        lease: Option<LeasePolicy>,
    ) -> CollectionConstraints {
        CollectionConstraints {
            unique: if unique { vec!["appId".into()] } else { vec![] },
            references,
            lease,
        }
    }

    #[test]
    fn large_cascade_tombstones_immediately_and_recovers_mid_job() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .configure_collection("a", "gurus".into(), policy(true, vec![], None))
            .unwrap();
        store
            .configure_collection(
                "a",
                "conversations".into(),
                policy(
                    true,
                    vec![ReferenceConstraint {
                        field: "guruId".into(),
                        collection: "gurus".into(),
                        target_field: "appId".into(),
                    }],
                    Some(LeasePolicy {
                        field: "appId".into(),
                        prefix: "conversation:".into(),
                    }),
                ),
            )
            .unwrap();
        store
            .configure_collection(
                "a",
                "messages".into(),
                policy(
                    true,
                    vec![ReferenceConstraint {
                        field: "conversationId".into(),
                        collection: "conversations".into(),
                        target_field: "appId".into(),
                    }],
                    Some(LeasePolicy {
                        field: "conversationId".into(),
                        prefix: "conversation:".into(),
                    }),
                ),
            )
            .unwrap();
        let guru = store
            .create("a", "gurus".into(), data(json!({"appId":"g"})), None)
            .unwrap();
        let lease = store
            .change_lease(
                "a",
                "conversation:c".into(),
                "run".into(),
                "acquire",
                None,
                None,
            )
            .unwrap();
        let proof = LeaseProof {
            resource: lease.resource.clone(),
            owner: lease.owner.clone(),
            fence: lease.fence,
        };
        let conversation = store
            .mutate(
                "a",
                "app",
                MutationRequest {
                    idempotency_key: "conversation".into(),
                    operations: vec![Mutation::Create {
                        collection: "conversations".into(),
                        data: data(json!({"appId":"c","guruId":"g"})),
                    }],
                    leases: vec![proof.clone()],
                },
            )
            .unwrap()
            .records
            .remove(0);
        for batch in 0..3 {
            let first = batch * 500;
            let operations = (first..(first + 500).min(1205))
                .map(|i| Mutation::Create {
                    collection: "messages".into(),
                    data: data(
                        json!({"appId":format!("m{i:04}"),"conversationId":"c","createdAt":i}),
                    ),
                })
                .collect();
            store
                .mutate(
                    "a",
                    "app",
                    MutationRequest {
                        idempotency_key: format!("batch-{batch}"),
                        operations,
                        leases: vec![proof.clone()],
                    },
                )
                .unwrap();
        }
        assert_eq!(store.list("a", Some("messages")).len(), 1205);
        assert_eq!(
            store
                .start_deletion_job("a", "app", "delete", &guru.id)
                .err()
                .unwrap()
                .to_string(),
            "lease_conflict"
        );
        store
            .change_lease(
                "a",
                lease.resource.clone(),
                lease.owner.clone(),
                "release",
                Some(lease.fence),
                None,
            )
            .unwrap();
        let job = store
            .start_deletion_job("a", "app", "delete", &guru.id)
            .unwrap();
        assert_eq!(job.total, 1207);
        assert_eq!(job.deleted, 0);
        assert_eq!(store.list("a", None).len(), 0);
        assert!(store.get_visible("a", &guru.id, None).is_none());
        assert!(
            store
                .indexed_visible_refs(
                    "a",
                    "messages",
                    &[("conversationId".into(), json!("c"))],
                    None
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .create(
                    "a",
                    "conversations".into(),
                    data(json!({"appId":"other","guruId":"g"})),
                    None
                )
                .err()
                .unwrap()
                .to_string(),
            "deletion_in_progress"
        );
        assert_eq!(
            store
                .change_lease(
                    "a",
                    "conversation:c".into(),
                    "new-run".into(),
                    "acquire",
                    None,
                    None
                )
                .err()
                .unwrap()
                .to_string(),
            "deletion_in_progress"
        );
        let progress = store.advance_deletion_job("a", &job.id).unwrap();
        assert_eq!(progress.deleted, 1000);
        assert_eq!(progress.status, "pending");
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(store.deletion_job("a", &job.id).unwrap().deleted, 1000);
        assert!(store.get_visible("a", &conversation.id, None).is_none());
        while store.deletion_job("a", &job.id).unwrap().status == "pending" {
            store.advance_deletion_job("a", &job.id).unwrap();
        }
        let complete = store.deletion_job("a", &job.id).unwrap();
        assert_eq!(complete.deleted, 1207);
        assert_eq!(complete.status, "complete");
        assert!(store.artifacts.is_empty());
        assert_eq!(
            store
                .start_deletion_job("a", "app", "delete", &guru.id)
                .unwrap()
                .status,
            "complete"
        );
        store.vacuum().unwrap();
        drop(store);
        let store = Store::open(&directory.0).unwrap();
        assert_eq!(store.deletion_job("a", &job.id).unwrap().status, "complete");
    }
}
