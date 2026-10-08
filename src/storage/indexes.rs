//! Rebuildable in-memory indexes for configured application fields.
use super::*;

impl Store {
    fn indexed_fields(&self, artifact: &Artifact) -> BTreeSet<String> {
        let Some(policy) = self
            .constraints
            .get(&format!("{}\0{}", artifact.workspace_id, artifact.kind))
        else {
            return BTreeSet::new();
        };
        let mut fields: BTreeSet<String> = policy.unique.iter().cloned().collect();
        fields.extend(
            policy
                .references
                .iter()
                .map(|reference| reference.field.clone()),
        );
        if let Some(lease) = &policy.lease {
            fields.insert(lease.field.clone());
        }
        fields
    }

    pub(super) fn index_artifact(&mut self, artifact: &Artifact) {
        for field in self.indexed_fields(artifact) {
            if let Some(value) = artifact.data.get(&field).and_then(Value::as_str) {
                self.field_index
                    .entry((
                        artifact.workspace_id.clone(),
                        artifact.kind.clone(),
                        field,
                        value.to_owned(),
                    ))
                    .or_default()
                    .insert(artifact.id.clone());
            }
        }
    }

    pub(super) fn unindex_artifact(&mut self, artifact: &Artifact) {
        for field in self.indexed_fields(artifact) {
            if let Some(value) = artifact.data.get(&field).and_then(Value::as_str) {
                let key = (
                    artifact.workspace_id.clone(),
                    artifact.kind.clone(),
                    field,
                    value.to_owned(),
                );
                if let Some(ids) = self.field_index.get_mut(&key) {
                    ids.remove(&artifact.id);
                    if ids.is_empty() {
                        self.field_index.remove(&key);
                    }
                }
            }
        }
    }

    pub(super) fn rebuild_collection_index(&mut self, workspace: &str, collection: &str) {
        let records: Vec<_> = self
            .artifacts
            .values()
            .filter(|a| a.workspace_id == workspace && a.kind == collection)
            .cloned()
            .collect();
        for artifact in records {
            self.index_artifact(&artifact);
        }
    }

    pub fn indexed_visible_refs<'a>(
        &'a self,
        workspace: &str,
        collection: &str,
        equalities: &[(String, Value)],
        user_id: Option<&str>,
    ) -> Option<Vec<&'a Arc<Artifact>>> {
        let policy = self
            .constraints
            .get(&format!("{workspace}\0{collection}"))?;
        let best = equalities
            .iter()
            .filter_map(|(field, value)| {
                let raw = value.as_str()?;
                if !policy.unique.contains(field)
                    && !policy.references.iter().any(|r| r.field == *field)
                    && !policy
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.field == *field)
                {
                    return None;
                }
                let key = (
                    workspace.to_owned(),
                    collection.to_owned(),
                    field.clone(),
                    raw.to_owned(),
                );
                Some(self.field_index.get(&key))
            })
            .min_by_key(|ids| ids.map_or(0, BTreeSet::len));
        let ids = best?;
        let now = Utc::now().to_rfc3339();
        Some(
            ids.into_iter()
                .flatten()
                .filter_map(|id| self.artifacts.get(id))
                .filter(|a| {
                    !Self::is_expired(a, &now)
                        && !self.blocked_records.contains(&a.id)
                        && self.is_record_visible(a, user_id, None)
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;
    use std::time::Instant;

    fn data(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn configured_indexes_rebuild_and_track_updates_deletes() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let record = store
            .create("a", "gurus".into(), data(json!({"appId":"g"})), None)
            .unwrap();
        store
            .configure_collection(
                "a",
                "gurus".into(),
                CollectionConstraints {
                    unique: vec!["appId".into()],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            store
                .indexed_visible_refs("a", "gurus", &[("appId".into(), json!("g"))], None)
                .unwrap()
                .len(),
            1
        );
        store
            .update(
                "a",
                &record.id,
                data(json!({"appId":"renamed"})),
                true,
                None,
            )
            .unwrap();
        assert!(
            store
                .indexed_visible_refs("a", "gurus", &[("appId".into(), json!("g"))], None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .indexed_visible_refs("a", "gurus", &[("appId".into(), json!("renamed"))], None)
                .unwrap()
                .len(),
            1
        );
        store.vacuum().unwrap();
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(
            store
                .indexed_visible_refs("a", "gurus", &[("appId".into(), json!("renamed"))], None)
                .unwrap()
                .len(),
            1
        );
        store.delete("a", &record.id, None).unwrap();
        assert!(
            store
                .indexed_visible_refs("a", "gurus", &[("appId".into(), json!("renamed"))], None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    #[ignore = "Run explicitly to measure realistic message page and preview reads"]
    fn benchmark_message_page_and_previews() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .configure_collection(
                "a",
                "conversations".into(),
                CollectionConstraints {
                    unique: vec!["appId".into()],
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .configure_collection(
                "a",
                "messages".into(),
                CollectionConstraints {
                    unique: vec!["appId".into()],
                    references: vec![ReferenceConstraint {
                        field: "conversationId".into(),
                        collection: "conversations".into(),
                        target_field: "appId".into(),
                    }],
                    lease: None,
                },
            )
            .unwrap();
        store
            .bulk_insert(
                "a",
                Some("conversations".into()),
                (0..100)
                    .map(|i| (None, data(json!({"appId":format!("c{i:03}")}))))
                    .collect(),
            )
            .unwrap();
        for chunk in 0..20 {
            store.bulk_insert("a", Some("messages".into()), (chunk*1000..(chunk+1)*1000).map(|i| (None, data(json!({"appId":format!("m{i:06}"),"conversationId":format!("c{:03}", i%100),"createdAt":i})))).collect()).unwrap();
        }
        let sql = "SELECT appId, createdAt FROM messages WHERE conversationId = $1 ORDER BY createdAt DESC, appId DESC LIMIT 31";
        let start = Instant::now();
        let mut scan_count = 0;
        for i in 0..100 {
            let value = json!(format!("c{i:03}"));
            let all = store.visible_refs("a", Some("messages"), None);
            scan_count += all.len();
            let _ = crate::query::execute_native_sql_refs(sql, &[value], &all, false).unwrap();
        }
        let scan_time = start.elapsed();
        let start = Instant::now();
        let mut index_count = 0;
        for i in 0..100 {
            let value = json!(format!("c{i:03}"));
            let indexed = store
                .indexed_visible_refs(
                    "a",
                    "messages",
                    &[("conversationId".into(), value.clone())],
                    None,
                )
                .unwrap();
            index_count += indexed.len();
            let _ = crate::query::execute_native_sql_refs(sql, &[value], &indexed, false).unwrap();
        }
        let index_time = start.elapsed();
        assert_eq!(scan_count, 2_000_000);
        assert_eq!(index_count, 20_000);
        eprintln!(
            "100 message pages: full scan {:?} ({} candidates), indexed {:?} ({} candidates)",
            scan_time, scan_count, index_time, index_count
        );
    }
}
