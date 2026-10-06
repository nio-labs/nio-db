use chrono::Utc;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

pub const MAX_JOURNAL_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

pub fn valid_bucket(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        && ![
            "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7",
            "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
        ]
        .contains(&name)
}

pub fn new_id(prefix: &str) -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{prefix}_{}", hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;

    #[test]
    fn cached_vectors_follow_updates_visibility_deletes_and_recovery() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let record = store.create("a", "vectors".into(), json!({"embedding":[1.0,0.0],"owner_id":"alice"}).as_object().unwrap().clone(), None).unwrap();
        assert!(store.search_vectors("a", Some("vectors"), &[1.0,0.0], 10, None, Some("bob")).is_empty());
        assert_eq!(store.search_vectors("a", Some("vectors"), &[1.0,0.0], 10, None, Some("alice"))[0].1, 1.0);
        store.update("a", &record.id, json!({"embedding":[0.0,1.0]}).as_object().unwrap().clone(), true, Some("alice")).unwrap();
        assert!(store.search_vectors("a", Some("vectors"), &[1.0,0.0], 10, Some(0.5), None).is_empty());
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(store.search_vectors("a", Some("vectors"), &[0.0,1.0], 1, None, None)[0].1, 1.0);
        assert!(store.search_vectors("a", Some("vectors"), &[0.0,1.0], 0, None, None).is_empty());
        store.update("a", &record.id, json!({"embedding":["bad",1]}).as_object().unwrap().clone(), true, None).unwrap();
        assert!(!store.vector_index.contains_key(&record.id));
        store.update("a", &record.id, json!({"embedding":[0,0]}).as_object().unwrap().clone(), true, None).unwrap();
        assert_eq!(store.search_vectors("a", Some("vectors"), &[0.0,1.0], 1, None, None)[0].1, 0.0);
        store.delete("a", &record.id, None).unwrap();
        assert!(store.vector_index.is_empty());
    }

    #[test]
    fn bulk_frame_is_atomic_recovers_and_repairs_derived_projections() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let base = store.create("a", "records".into(), serde_json::json!({"value":0}).as_object().unwrap().clone(), None).unwrap();
        let prefix = fs::read(directory.0.join("journal.jsonl")).unwrap();
        let prefix_head = fs::read(directory.0.join("ledger-head.json")).unwrap();
        let items = || (1..=25).map(|i| (Some("records".into()), serde_json::json!({"value":i,"ttl":3600}).as_object().unwrap().clone())).collect();
        let mut invalid: Vec<_> = items();
        invalid.push((Some("".into()), Map::new()));
        assert!(store.bulk_insert("a", None, invalid).is_err());
        assert_eq!(store.list("a", None).len(), 1);
        assert_eq!(fs::read(directory.0.join("journal.jsonl")).unwrap(), prefix);
        let result = store.bulk_insert("a", None, items()).unwrap();
        assert_eq!(result.len(), 25);
        assert!(result.iter().all(|a| a.data.get("expires_at").is_some()));
        let committed = fs::read(directory.0.join("journal.jsonl")).unwrap();
        assert_eq!(committed.iter().filter(|&&b| b == b'\n').count(), 2);
        let cache = directory.0.join("artifacts").join(format!("{}.toon", result[0].id));
        fs::write(&cache, "damaged cache").unwrap();
        drop(store);
        let store = Store::open(&directory.0).unwrap();
        assert_eq!(store.list("a", None).len(), 26);
        assert_ne!(fs::read_to_string(&cache).unwrap(), "damaged cache");
        drop(store);
        // A crash before the final frame/checkpoint becomes durable leaves the batch absent.
        let partial = &committed[..prefix.len() + (committed.len() - prefix.len()) / 2];
        fs::write(directory.0.join("journal.jsonl"), partial).unwrap();
        fs::write(directory.0.join("ledger-head.json"), prefix_head).unwrap();
        let store = Store::open(&directory.0).unwrap();
        assert_eq!(store.list("a", None).len(), 1);
        assert_eq!(store.list("a", None)[0].id, base.id);
        assert_eq!(fs::read(directory.0.join("journal.jsonl")).unwrap(), prefix);
        assert!(!cache.exists());
    }

    #[test]
    fn snapshots_preserve_versions_and_paging_clones_only_visible_results() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let records = store.bulk_insert("a", Some("records".into()), (0..10).map(|i| (None, serde_json::json!({"owner_id":if i%2==0 {"alice"} else {"bob"},"value":i}).as_object().unwrap().clone())).collect()).unwrap();
        let old = store.snapshot_visible("a", None, Some("alice"));
        assert_eq!(old.len(), 5);
        let id = old[0].id.clone();
        store.update("a", &id, serde_json::json!({"value":99}).as_object().unwrap().clone(), true, Some("alice")).unwrap();
        assert_ne!(old[0].data["value"], 99);
        assert_eq!(store.get("a", &id).unwrap().data["value"], 99);
        let (page,total,more) = store.page_visible("a", None, Some("alice"), None, 1, 2).unwrap();
        assert_eq!((page.len(),total,more), (2,5,true));
        let anchor = page.last().unwrap();
        let (next,_,more) = store.page_visible("a", None, Some("alice"), Some((&anchor.created_at,&anchor.id)), 0, 2).unwrap();
        assert_eq!(next.len(), 2);
        assert!(!more);
        assert!(store.page_visible("a", None, Some("bob"), Some((&anchor.created_at,&anchor.id)), 0, 2).is_none());
        assert_eq!(store.page_visible("a", None, None, None, usize::MAX, 2).unwrap().0.len(), 0);
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn recovery_preserves_commits_and_discards_only_partial_tail() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        assert!(Store::open(&directory.0).is_err());
        let record = store
            .create(
                "a",
                "tasks".into(),
                json!({"status":"pending"}).as_object().unwrap().clone(),
                Some("retry".into()),
            )
            .unwrap();
        let length = fs::metadata(directory.0.join("journal.jsonl"))
            .unwrap()
            .len();
        let backup = directory.0.join("backup.jsonl");
        store.backup(&backup).unwrap();
        drop(store);
        fs::remove_file(
            directory
                .0
                .join("artifacts")
                .join(format!("{}.toon", record.id)),
        )
        .unwrap();
        OpenOptions::new()
            .append(true)
            .open(directory.0.join("journal.jsonl"))
            .unwrap()
            .write_all(b"{\"partial\":")
            .unwrap();
        let mut recovered = Store::open(&directory.0).unwrap();
        assert_eq!(recovered.get("a", &record.id).unwrap().data, record.data);
        assert!(recovered.get("b", &record.id).is_none());
        assert!(
            directory
                .0
                .join("artifacts")
                .join(format!("{}.toon", record.id))
                .exists()
        );
        assert_eq!(
            fs::metadata(directory.0.join("journal.jsonl"))
                .unwrap()
                .len(),
            length
        );
        assert_eq!(
            recovered
                .create(
                    "a",
                    "tasks".into(),
                    record.data.clone(),
                    Some("retry".into())
                )
                .unwrap()
                .id,
            record.id
        );
        let conflict = recovered
            .create("a", "tasks".into(), Map::new(), Some("retry".into()))
            .unwrap_err();
        assert_eq!(conflict.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read(backup).unwrap(),
            fs::read(directory.0.join("journal.jsonl")).unwrap()
        );
    }

    #[test]
    fn complete_corrupt_frame_fails_without_erasing_data() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.create("a", "tasks".into(), Map::new(), None).unwrap();
        drop(store);
        let path = directory.0.join("journal.jsonl");
        let original = fs::read(&path).unwrap();
        let mut altered = original.clone();
        let index = altered.windows(4).position(|w| w == b"task").unwrap();
        altered[index] = b'X';
        fs::write(&path, &altered).unwrap();
        assert!(Store::open(&directory.0).is_err());
        assert_eq!(fs::read(path).unwrap(), altered);
    }

    #[test]
    fn upload_rejects_owner_deleted_before_commit() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let user = store
            .create_user("a".into(), "owner".into(), "unused".into())
            .unwrap();
        let mut ticket = store.begin_upload().unwrap();
        ticket.file.write_all(b"x").unwrap();
        ticket.file.sync_all().unwrap();
        store.delete_user("a", &user.id).unwrap();
        let file = StoredFile {
            id: new_id("file"),
            artifact_id: String::new(),
            workspace_id: "a".into(),
            bucket: "documents".into(),
            filename: "x.txt".into(),
            sha256: hex(&Sha256::digest(b"x")),
            size: 1,
            mime: "text/plain".into(),
            created_at: Utc::now().to_rfc3339(),
            created_by: "backend".into(),
            user_id: Some(user.id),
        };
        assert_eq!(
            store.finish_upload(file, &ticket).err().unwrap().kind(),
            io::ErrorKind::NotFound
        );
        assert!(store.files("a", "documents").is_empty());
    }

    #[test]
    fn users_lists_and_filters_by_workspace() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        assert!(store.users("ws_a").is_empty());

        let user1 = store
            .create_user("ws_a".into(), "alice".into(), "hash1".into())
            .unwrap();
        let user2 = store
            .create_user("ws_a".into(), "bob".into(), "hash2".into())
            .unwrap();
        let _user_other = store
            .create_user("ws_b".into(), "charlie".into(), "hash3".into())
            .unwrap();

        let ws_a_users = store.users("ws_a");
        assert_eq!(ws_a_users.len(), 2);
        assert!(ws_a_users.iter().any(|u| u.id == user1.id && u.username == "alice"));
        assert!(ws_a_users.iter().any(|u| u.id == user2.id && u.username == "bob"));

        let ws_b_users = store.users("ws_b");
        assert_eq!(ws_b_users.len(), 1);
        assert_eq!(ws_b_users[0].username, "charlie");

        store.delete_user("ws_a", &user1.id).unwrap();
        assert_eq!(store.users("ws_a").len(), 1);
    }

    #[test]
    fn merkle_provenance_hash_chain_and_verify() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(store.last_seq(), 0);
        assert_eq!(store.root_hash(), GENESIS_HASH);

        let r1 = store.create("w1", "items".into(), json!({"val": 1}).as_object().unwrap().clone(), None).unwrap();
        assert_eq!(store.last_seq(), 1);
        let h1 = store.root_hash().to_string();
        assert_ne!(h1, GENESIS_HASH);

        let _r2 = store.create("w1", "items".into(), json!({"val": 2}).as_object().unwrap().clone(), None).unwrap();
        assert_eq!(store.last_seq(), 2);
        let h2 = store.root_hash().to_string();
        assert_ne!(h2, h1);

        store.delete("w1", &r1.id, None).unwrap();
        assert_eq!(store.last_seq(), 3);
        let h3 = store.root_hash().to_string();
        assert_ne!(h3, h2);

        let report = store.verify_ledger().unwrap();
        assert!(report.verified);
        assert!(!report.tampering_detected);
        assert_eq!(report.total_frames, 3);
        assert_eq!(report.latest_seq, 3);
        assert_eq!(report.root_hash, h3);
        assert_eq!(report.genesis_hash, GENESIS_HASH);

        // Reopening store re-verifies the chain and keeps head hash
        drop(store);
        let store2 = Store::open(&directory.0).unwrap();
        assert_eq!(store2.last_seq(), 3);
        assert_eq!(store2.root_hash(), h3);
    }

    #[test]
    fn merkle_provenance_detects_frame_omission_and_reordering() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        for i in 1..=3 {
            store.create("w", "items".into(), json!({"i": i}).as_object().unwrap().clone(), None).unwrap();
        }
        drop(store);

        let journal_path = directory.0.join("journal.jsonl");
        let content = fs::read_to_string(&journal_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Case 1: Omission (delete middle frame)
        let omitted = format!("{}\n{}\n", lines[0], lines[2]);
        fs::write(&journal_path, omitted).unwrap();
        assert!(Store::open(&directory.0).is_err());

        // Case 2: Reordering (swap frame 1 and 2)
        let swapped = format!("{}\n{}\n{}\n", lines[1], lines[0], lines[2]);
        fs::write(&journal_path, swapped).unwrap();
        assert!(Store::open(&directory.0).is_err());

        // Case 3: In-place tampering
        let tampered = format!("{}\n{}\n{}\n", lines[0].replace("\"i\":1", "\"i\":99"), lines[1], lines[2]);
        fs::write(&journal_path, tampered).unwrap();
        assert!(Store::open(&directory.0).is_err());
    }

    #[test]
    fn merkle_provenance_vacuum_preserves_valid_chain() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let r1 = store.create("w", "notes".into(), json!({"text": "hello"}).as_object().unwrap().clone(), None).unwrap();
        let _r2 = store.create("w", "notes".into(), json!({"text": "world"}).as_object().unwrap().clone(), None).unwrap();
        store.delete("w", &r1.id, None).unwrap();
        assert_eq!(store.last_seq(), 3);

        let stats = store.vacuum().unwrap();
        assert_eq!(stats.active_artifacts, 1);
        assert_eq!(store.last_seq(), 5); // reset + active artifact follow the old head

        let report = store.verify_ledger().unwrap();
        assert!(report.verified);
        assert!(!report.tampering_detected);
        assert_eq!(report.total_frames, 5);

        drop(store);
        let store2 = Store::open(&directory.0).unwrap();
        assert_eq!(store2.last_seq(), 5);
        assert!(store2.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_rejects_duplicate_gap_and_missing_sequence() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        for i in 0..3 {
            store
                .create(
                    "w",
                    "items".into(),
                    json!({"i": i}).as_object().unwrap().clone(),
                    None,
                )
                .unwrap();
        }
        let path = directory.0.join("journal.jsonl");
        let original = fs::read_to_string(&path).unwrap();
        let entries: Vec<Entry> = original
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for sequence in [Some(1), Some(3), None] {
            let mut modified = entries.clone();
            modified[1].seq = sequence;
            // Recompute the hash so the sequence check itself must reject it.
            if let Some(seq) = sequence {
                modified[1].hash = Some(compute_frame_hash(
                    seq,
                    modified[1].prev_hash.as_deref().unwrap(),
                    &serde_json::to_vec(&modified[1].payload).unwrap(),
                ));
            }
            let bytes: String = modified
                .iter()
                .map(|entry| format!("{}\n", serde_json::to_string(entry).unwrap()))
                .collect();
            fs::write(&path, bytes).unwrap();
            let report = store.verify_ledger().unwrap();
            assert!(!report.verified);
            assert!(report.tampering_detected);
            assert!(report.error.unwrap().contains("sequence discontinuity"));
            assert!(Store::open(&directory.0).is_err());
        }
        fs::write(path, original).unwrap();
    }

    #[test]
    fn merkle_v2_sequence_continues_after_restart_and_failed_validation() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        let root = store.root_hash().to_string();
        assert!(
            store
                .bulk_insert("w", None, vec![(Some("".into()), Map::new())])
                .is_err()
        );
        assert_eq!(store.last_seq(), 1);
        assert_eq!(store.root_hash(), root);
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        let content = fs::read_to_string(directory.0.join("journal.jsonl")).unwrap();
        let entries: Vec<Entry> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(entries.iter().all(|entry| entry.version == 2));
        assert_eq!(entries[1].seq, Some(2));
        assert_eq!(entries[1].prev_hash.as_deref(), Some(root.as_str()));
        assert!(store.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_complete_tail_deletion_rejected_live_and_after_restart() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        for _ in 0..3 {
            store.create("w", "items".into(), Map::new(), None).unwrap();
        }
        let path = directory.0.join("journal.jsonl");
        let original_root = store.root_hash().to_string();
        let content = fs::read_to_string(&path).unwrap();
        let shortened = format!(
            "{}\n",
            content.lines().take(2).collect::<Vec<_>>().join("\n")
        );
        fs::write(&path, shortened).unwrap();
        let report = store.verify_ledger().unwrap();
        assert!(!report.verified);
        assert!(report.tampering_detected);
        assert_eq!(report.latest_seq, 2);
        assert_ne!(report.root_hash, original_root);
        drop(store);
        assert!(Store::open(&directory.0).is_err());
    }

    #[test]
    fn merkle_v2_legacy_downgrade_rejected() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        for i in 0..3 {
            store
                .create(
                    "w",
                    "items".into(),
                    json!({"i": i}).as_object().unwrap().clone(),
                    None,
                )
                .unwrap();
        }
        let path = directory.0.join("journal.jsonl");
        let content = fs::read_to_string(&path).unwrap();
        let mut entries: Vec<Entry> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        // Retain a genuine v2 prefix, then replace its suffix with legacy frames.
        // Legacy checksums have no dependency on the preceding frame.
        for entry in &mut entries[1..] {
            entry.version = 1;
            entry.seq = None;
            entry.prev_hash = None;
            entry.hash = None;
            entry.sha256 = Some(hex(&Sha256::digest(
                serde_json::to_vec(&entry.payload).unwrap(),
            )));
        }
        entries.swap(1, 2);
        let bytes: String = entries
            .iter()
            .map(|entry| format!("{}\n", serde_json::to_string(entry).unwrap()))
            .collect();
        fs::write(path, bytes).unwrap();
        let report = store.verify_ledger().unwrap();
        assert!(!report.verified);
        assert!(report.error.unwrap().contains("downgrade"));
        drop(store);
        assert!(Store::open(&directory.0).is_err());
    }

    #[test]
    fn merkle_v2_legacy_prefix_migrates_and_survives_vacuum() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        drop(store);
        let path = directory.0.join("journal.jsonl");
        let mut entry: Entry = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        entry.version = 1;
        entry.seq = None;
        entry.prev_hash = None;
        entry.hash = None;
        entry.sha256 = Some(hex(&Sha256::digest(
            serde_json::to_vec(&entry.payload).unwrap(),
        )));
        fs::write(
            path,
            format!("{}\n", serde_json::to_string(&entry).unwrap()),
        )
        .unwrap();
        fs::remove_file(directory.0.join("ledger-head.json")).unwrap(); // pre-checkpoint legacy fixture
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        assert_eq!(store.last_seq(), 2);
        assert!(store.verify_ledger().unwrap().verified);
        store.vacuum().unwrap();
        let seq = store.last_seq();
        drop(store);
        let store = Store::open(&directory.0).unwrap();
        assert_eq!(store.last_seq(), seq);
        assert_eq!(store.list("w", None).len(), 2);
        assert!(store.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_repeated_vacuum_retains_audit_idempotency_and_restorable_backup() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let data = json!({"value": 1}).as_object().unwrap().clone();
        let record = store
            .create("w", "items".into(), data.clone(), Some("retry".into()))
            .unwrap();
        let deleted = store.create("w", "items".into(), Map::new(), None).unwrap();
        store.delete("w", &deleted.id, None).unwrap();
        let original = fs::read(directory.0.join("journal.jsonl")).unwrap();
        let original_hash = store.root_hash().to_string();
        let mut previous_seq = store.last_seq();
        for _ in 0..3 {
            store.vacuum().unwrap();
            assert!(store.last_seq() > previous_seq);
            previous_seq = store.last_seq();
            assert_eq!(
                store
                    .create("w", "items".into(), data.clone(), Some("retry".into()))
                    .unwrap()
                    .id,
                record.id
            );
            assert_eq!(store.last_seq(), previous_seq); // retry remains a no-op
            assert!(store.verify_ledger().unwrap().verified);
        }
        assert_eq!(
            fs::read(
                directory
                    .0
                    .join("journal-history")
                    .join(format!("{original_hash}.jsonl"))
            )
            .unwrap(),
            original
        );
        let restored = Directory::new();
        store.backup(&restored.0.join("journal.jsonl")).unwrap();
        let mut backup = Store::open(&restored.0).unwrap();
        assert_eq!(backup.last_seq(), store.last_seq());
        assert_eq!(backup.root_hash(), store.root_hash());
        assert_eq!(backup.list("w", None).len(), 1);
        assert!(backup.get("w", &deleted.id).is_none());
        assert_eq!(
            backup
                .create("w", "items".into(), data, Some("retry".into()))
                .unwrap()
                .id,
            record.id
        );
        let full_backup = directory.0.join("full-backup");
        store.backup_full(&full_backup).unwrap();
        let restored = Store::open(&full_backup).unwrap();
        assert_eq!(restored.last_seq(), store.last_seq());
        assert_eq!(restored.root_hash(), store.root_hash());
        assert_eq!(restored.list("w", None).len(), 1);
        assert!(restored.verify_ledger().unwrap().verified);
        assert!(
            full_backup
                .join("journal-history")
                .join(format!("{original_hash}.jsonl"))
                .exists()
        );
        drop(store);
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(store.last_seq(), previous_seq);
        store.create("w", "items".into(), Map::new(), None).unwrap();
        assert_eq!(store.last_seq(), previous_seq + 1);
    }

    #[test]
    fn merkle_v2_archived_tampering_and_missing_history_rejected() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store
            .create(
                "w",
                "items".into(),
                json!({"value": 1}).as_object().unwrap().clone(),
                None,
            )
            .unwrap();
        let hash = store.root_hash().to_string();
        store.vacuum().unwrap();
        let archive = directory
            .0
            .join("journal-history")
            .join(format!("{hash}.jsonl"));
        let original = fs::read_to_string(&archive).unwrap();
        fs::write(&archive, original.replace("\"value\":1", "\"value\":2")).unwrap();
        assert!(!store.verify_ledger().unwrap().verified);
        drop(store);
        assert!(Store::open(&directory.0).is_err());
        fs::remove_file(&archive).unwrap();
        assert!(Store::open(&directory.0).is_err());
    }

    #[test]
    fn merkle_v2_external_checkpoint_rejects_directory_rollback() {
        let directory = Directory::new();
        let trusted = Directory::new();
        let checkpoint = trusted.0.join("head.json");
        let mut store = Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        let old_journal = fs::read(directory.0.join("journal.jsonl")).unwrap();
        let old_head = fs::read(directory.0.join("ledger-head.json")).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        store.vacuum().unwrap();
        drop(store);
        fs::write(directory.0.join("journal.jsonl"), old_journal).unwrap();
        fs::write(directory.0.join("ledger-head.json"), old_head).unwrap();
        let error = Store::open_with_checkpoint(&directory.0, Some(&checkpoint))
            .err()
            .unwrap();
        assert!(error.to_string().contains("rollback"));
    }

    #[test]
    fn merkle_v2_recovery_accepts_durable_frames_ahead_of_checkpoint() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        let old_head = fs::read(directory.0.join("ledger-head.json")).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        store.vacuum().unwrap();
        let latest_seq = store.last_seq();
        let latest_hash = store.root_hash().to_string();
        drop(store);
        // Simulate a crash after journal replacement but before head publication.
        fs::write(directory.0.join("ledger-head.json"), old_head).unwrap();
        OpenOptions::new()
            .append(true)
            .open(directory.0.join("journal.jsonl"))
            .unwrap()
            .write_all(b"{\"partial\":")
            .unwrap();
        let store = Store::open(&directory.0).unwrap();
        assert_eq!(store.last_seq(), latest_seq);
        assert_eq!(store.root_hash(), latest_hash);
        assert!(store.verify_ledger().unwrap().verified);
        let head = read_checkpoint(&directory.0.join("ledger-head.json"))
            .unwrap()
            .unwrap();
        assert_eq!(head.latest_seq, latest_seq);
    }

    #[test]
    fn merkle_v2_recovery_never_truncates_acknowledged_frame() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        drop(store);
        let path = directory.0.join("journal.jsonl");
        let bytes = fs::read(&path).unwrap();
        let partial = &bytes[..bytes.len() / 2];
        fs::write(&path, partial).unwrap();
        assert!(Store::open(&directory.0).is_err());
        assert_eq!(fs::read(path).unwrap(), partial);
    }

    #[test]
    fn merkle_v2_failed_vacuum_leaves_live_state_and_journal_unchanged() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let expired = store
            .create(
                "w",
                "items".into(),
                json!({"expires_at":"2000-01-01T00:00:00Z"})
                    .as_object()
                    .unwrap()
                    .clone(),
                None,
            )
            .unwrap();
        let original = fs::read(directory.0.join("journal.jsonl")).unwrap();
        let original_hash = store.root_hash().to_string();
        fs::write(
            directory.0.join("journal-history"),
            "block archive directory",
        )
        .unwrap();
        assert!(store.vacuum().is_err());
        assert!(store.artifacts.contains_key(&expired.id));
        assert_eq!(store.root_hash(), original_hash);
        assert_eq!(
            fs::read(directory.0.join("journal.jsonl")).unwrap(),
            original
        );
        assert!(store.healthy);
        store.create("w", "items".into(), Map::new(), None).unwrap();
        assert!(store.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_sequence_overflow_fails_without_writing() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let original = fs::read(directory.0.join("journal.jsonl")).unwrap();
        store.last_seq = u64::MAX;
        assert!(store.create("w", "items".into(), Map::new(), None).is_err());
        assert_eq!(
            fs::read(directory.0.join("journal.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn merkle_v2_external_checkpoint_rejects_rehashed_history() {
        let directory = Directory::new();
        let trusted = Directory::new();
        let checkpoint = trusted.0.join("head.json");
        let mut store = Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).unwrap();
        for i in 0..3 {
            store
                .create(
                    "w",
                    "items".into(),
                    json!({"i":i}).as_object().unwrap().clone(),
                    None,
                )
                .unwrap();
        }
        drop(store);
        let path = directory.0.join("journal.jsonl");
        let content = fs::read_to_string(&path).unwrap();
        let mut entries: Vec<Entry> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        entries[0].payload["artifact"]["data"]["i"] = json!(99);
        let mut hash = GENESIS_HASH.to_string();
        for entry in &mut entries {
            entry.prev_hash = Some(hash.clone());
            hash = compute_frame_hash(
                entry.seq.unwrap(),
                &hash,
                &serde_json::to_vec(&entry.payload).unwrap(),
            );
            entry.hash = Some(hash.clone());
        }
        fs::write(
            path,
            entries
                .iter()
                .map(|entry| format!("{}\n", serde_json::to_string(entry).unwrap()))
                .collect::<String>(),
        )
        .unwrap();
        // The attacker rewrites the local head along with a self-consistent chain.
        write_checkpoint(
            &directory.0.join("ledger-head.json"),
            &LedgerCheckpoint {
                latest_seq: 3,
                root_hash: hash,
            },
        )
        .unwrap();
        let err = Store::open_with_checkpoint(&directory.0, Some(&checkpoint))
            .err()
            .unwrap();
        assert!(err.to_string().contains("checkpoint hash mismatch"));
    }

    #[test]
    fn merkle_v2_checkpoint_failure_blocks_further_writes_until_recovery() {
        let directory = Directory::new();
        let trusted = Directory::new();
        let checkpoint = trusted.0.join("head.json");
        let mut store = Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).unwrap();
        store.create("w", "items".into(), Map::new(), None).unwrap();
        let old_head = fs::read(&checkpoint).unwrap();
        fs::remove_file(&checkpoint).unwrap();
        fs::create_dir(&checkpoint).unwrap(); // inject a checkpoint publication failure
        assert!(store.create("w", "items".into(), Map::new(), None).is_err());
        assert!(!store.healthy);
        assert_eq!(store.artifacts.len(), 1);
        let committed = fs::read(directory.0.join("journal.jsonl")).unwrap();
        assert!(store.create("w", "items".into(), Map::new(), None).is_err());
        assert!(store.vacuum().is_err());
        assert_eq!(
            fs::read(directory.0.join("journal.jsonl")).unwrap(),
            committed
        );
        drop(store);
        fs::remove_dir(&checkpoint).unwrap();
        fs::write(&checkpoint, old_head).unwrap();
        let store = Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).unwrap();
        assert_eq!(store.artifacts.len(), 2); // a synced but unacknowledged write can recover
        assert!(store.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_external_checkpoint_is_outside_data_and_exclusively_owned() {
        let directory = Directory::new();
        assert!(
            Store::open_with_checkpoint(&directory.0, Some(&directory.0.join("head.json")))
                .is_err()
        );
        let other = Directory::new();
        let trusted = Directory::new();
        let checkpoint = trusted.0.join("head.json");
        let store = Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).unwrap();
        let err = Store::open_with_checkpoint(&other.0, Some(&checkpoint))
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("another server owns this ledger checkpoint")
        );
        drop(store);
        assert!(Store::open_with_checkpoint(&directory.0, Some(&checkpoint)).is_ok());
    }

    #[test]
    fn merkle_v2_recovery_before_compaction_swap_ignores_staged_generation() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let record = store.create("w", "items".into(), Map::new(), None).unwrap();
        let hash = store.root_hash().to_string();
        drop(store);
        let history = directory.0.join("journal-history");
        secure_dir(&history).unwrap();
        fs::copy(
            directory.0.join("journal.jsonl"),
            history.join(format!("{hash}.jsonl")),
        )
        .unwrap();
        fs::write(
            directory.0.join("journal.compact_interrupted"),
            "incomplete staged snapshot",
        )
        .unwrap();
        let mut store = Store::open(&directory.0).unwrap();
        assert_eq!(store.last_seq(), 1);
        assert_eq!(store.root_hash(), hash);
        assert_eq!(store.get("w", &record.id).unwrap().id, record.id);
        store.vacuum().unwrap();
        assert!(store.verify_ledger().unwrap().verified);
    }

    #[test]
    fn merkle_v2_full_backup_after_vacuum_restores_uploaded_blobs() {
        let directory = Directory::new();
        let mut store = Store::open(&directory.0).unwrap();
        let bytes = b"retained blob";
        let mut ticket = store.begin_upload().unwrap();
        ticket.file.write_all(bytes).unwrap();
        ticket.file.sync_all().unwrap();
        let file = store
            .finish_upload(
                StoredFile {
                    id: new_id("file"),
                    artifact_id: String::new(),
                    workspace_id: "w".into(),
                    bucket: "documents".into(),
                    filename: "test.txt".into(),
                    sha256: hex(&Sha256::digest(bytes)),
                    size: bytes.len() as u64,
                    mime: "text/plain".into(),
                    created_at: Utc::now().to_rfc3339(),
                    created_by: "backend".into(),
                    user_id: None,
                },
                &ticket,
            )
            .unwrap();
        store.vacuum().unwrap();
        let destination = directory.0.join("full-backup");
        store.backup_full(&destination).unwrap();
        let backup = Store::open(&destination).unwrap();
        assert_eq!(backup.last_seq(), store.last_seq());
        assert_eq!(backup.root_hash(), store.root_hash());
        assert_eq!(backup.files("w", "documents")[0].id, file.id);
        assert_eq!(fs::read(backup.blob_path(&file).unwrap()).unwrap(), bytes);
        assert!(backup.verify_ledger().unwrap().verified);
    }

}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (&x, &y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let norm = norm_a * norm_b;
    if norm <= 0.0 {
        return 0.0;
    }
    dot / norm.sqrt()
}

pub fn secure_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::other(
            "data directories cannot be symbolic links",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn private_file(path: &Path, create_new: bool) -> io::Result<File> {
    if path.exists() && fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::other("database files cannot be symbolic links"));
    }
    let mut opts = OpenOptions::new();
    opts.read(true).write(true);
    if create_new {
        opts.create_new(true);
    } else {
        opts.create(true).truncate(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
#[cfg(not(unix))]
pub(crate) fn sync_dir(_: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub workspace_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub data: Map<String, Value>,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub owner: String,
    pub workspace_id: String,
    pub messages: Vec<(String, String)>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub id: String,
    pub workspace_id: String,
    pub username: String,
    pub password_hash: String,
    pub created_at: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub token_sha256: String,
    pub user_id: String,
    pub expires_at: i64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub workspace_id: String,
    pub name: String,
    pub created_at: String,
    pub created_by: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredFile {
    pub id: String,
    pub artifact_id: String,
    pub workspace_id: String,
    pub bucket: String,
    pub filename: String,
    pub sha256: String,
    pub size: u64,
    pub mime: String,
    pub created_at: String,
    pub created_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventWorker {
    pub id: String,
    #[serde(alias = "event_type")]
    pub event_name: String,
    pub source: String,
    pub created_at: String,
    pub created_by: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    #[serde(alias = "event_type")]
    pub event_name: String,
    pub url: String,
    pub secret: String,
    pub created_at: String,
    pub created_by: String,
}

pub struct UploadTicket {
    pub path: PathBuf,
    pub file: File,
}
impl Drop for UploadTicket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    SnapshotReset {
        idempotency: BTreeMap<String, (String, String)>,
    },
    Batch { events: Vec<Event> },
    Artifact {
        artifact: Artifact,
        idempotency: Option<String>,
        fingerprint: String,
    },
    ArtifactDeleted {
        id: String,
    },
    Conversation {
        conversation: Conversation,
    },
    UserCreated {
        user: UserRecord,
    },
    UserDeleted {
        id: String,
    },
    SessionCreated {
        session: UserSession,
    },
    SessionRevoked {
        token_sha256: String,
    },
    BucketCreated {
        bucket: Bucket,
    },
    FileUploaded {
        file: StoredFile,
        artifact: Artifact,
    },
    FileDeleted {
        id: String,
    },
    WorkerCreated {
        worker: EventWorker,
    },
    WorkerDeleted {
        id: String,
    },
    DemoSeeded {
        payload: Value,
    },
    WebhookCreated {
        webhook: Webhook,
    },
    WebhookDeleted {
        id: String,
    },
}

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

pub fn compute_frame_hash(seq: u64, prev_hash: &str, payload_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(seq.to_string().as_bytes());
    hasher.update(b":");
    hasher.update(prev_hash.as_bytes());
    hasher.update(b":");
    hasher.update(payload_bytes);
    hex(&hasher.finalize())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LedgerReport {
    pub verified: bool,
    pub total_frames: u64,
    pub root_hash: String,
    pub genesis_hash: String,
    pub latest_seq: u64,
    pub tampering_detected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerCheckpoint {
    latest_seq: u64,
    root_hash: String,
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_checkpoint(path: &Path) -> io::Result<Option<LedgerCheckpoint>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(io::Error::other("ledger checkpoint must be a regular file"));
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    }
    match fs::read(path) {
        Ok(bytes) => {
            let head: LedgerCheckpoint =
                serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if !valid_hash(&head.root_hash) {
                return Err(io::Error::other("invalid ledger checkpoint hash"));
            }
            Ok(Some(head))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_checkpoint(path: &Path, head: &LedgerCheckpoint) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temporary = parent.join(new_id("ledger-head"));
    let result = (|| {
        let mut file = private_file(&temporary, true)?;
        serde_json::to_writer(&mut file, head).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_dir(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

// A compacted journal begins at a higher sequence and links to the archived
// segment through its first frame's authenticated prev_hash. Resolve backwards
// iteratively, then verify forwards; never trust archive filenames as evidence.
fn journal_segments(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut segments = vec![root.join("journal.jsonl")];
    let mut previous_start = None;
    loop {
        let mut reader = BufReader::new(File::open(segments.last().unwrap())?);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line)?;
        if line.is_empty() || line.last() != Some(&b'\n') {
            break;
        }
        let entry: Entry = serde_json::from_slice(&line).map_err(io::Error::other)?;
        if entry.version != 2 {
            break;
        }
        let seq = entry
            .seq
            .ok_or_else(|| io::Error::other("missing first-frame sequence"))?;
        if previous_start.is_some_and(|previous| seq >= previous) {
            return Err(io::Error::other("cyclic or non-decreasing journal archive"));
        }
        previous_start = Some(seq);
        if seq <= 1 {
            break;
        }
        let hash = entry
            .prev_hash
            .filter(|h| valid_hash(h))
            .ok_or_else(|| io::Error::other("invalid archive hash"))?;
        if !fs::symlink_metadata(root.join("journal-history"))?.file_type().is_dir() {
            return Err(io::Error::other("journal history must be a regular directory"));
        }
        let path = root.join("journal-history").join(format!("{hash}.jsonl"));
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(io::Error::other("journal archive must be a regular file"));
        }
        segments.push(path);
    }
    segments.reverse();
    Ok(segments)
}

fn scan_ledger(
    root: &Path,
    checkpoints: &[LedgerCheckpoint],
    repair_tail: bool,
) -> io::Result<LedgerReport> {
    let mut report = LedgerReport {
        verified: true,
        total_frames: 0,
        root_hash: GENESIS_HASH.into(),
        genesis_hash: GENESIS_HASH.into(),
        latest_seq: 0,
        tampering_detected: false,
        error: None,
    };
    let mut failure = |message: String| {
        report.verified = false;
        report.tampering_detected = true;
        report.error = Some(message);
        report.clone()
    };
    let segments = match journal_segments(root) {
        Ok(paths) => paths,
        Err(err) => return Ok(failure(format!("invalid journal history: {err}"))),
    };
    let mut seen_v2 = false;
    let mut matched = vec![false; checkpoints.len()];
    for (index, head) in checkpoints.iter().enumerate() {
        if head.latest_seq == 0 {
            if head.root_hash != GENESIS_HASH {
                return Ok(failure("invalid genesis checkpoint".into()));
            }
            matched[index] = true;
        }
    }
    let mut truncate_at = None;
    for (segment_index, path) in segments.iter().enumerate() {
        let mut reader = BufReader::new(File::open(path)?);
        let mut offset = 0;
        loop {
            let mut line = Vec::new();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            let error = if line.last() != Some(&b'\n') {
                if repair_tail && segment_index + 1 == segments.len() {
                    truncate_at = Some(offset);
                    break;
                }
                Some("trailing uncommitted or incomplete frame".to_string())
            } else {
                None
            };
            if let Some(error) = error {
                report.error = Some(error);
                break;
            }
            let entry: Entry = match serde_json::from_slice(&line) {
                Ok(entry) => entry,
                Err(err) => {
                    report.error = Some(format!("corrupt entry json: {err}"));
                    break;
                }
            };
            let payload = serde_json::to_vec(&entry.payload).map_err(io::Error::other)?;
            let next = report
                .latest_seq
                .checked_add(1)
                .ok_or_else(|| io::Error::other("ledger sequence exhausted"))?;
            let hash = match entry.version {
                1 if seen_v2 => {
                    report.error = Some("legacy v1 frame after v2: downgrade rejected".into());
                    break;
                }
                1 => {
                    let hash = hex(&Sha256::digest(&payload));
                    if entry.sha256.as_deref() != Some(&hash) {
                        report.error =
                            Some(format!("sha256 checksum failure at legacy frame {next}"));
                        break;
                    }
                    hash
                }
                2 => {
                    seen_v2 = true;
                    if entry.seq != Some(next) {
                        report.error = Some(format!(
                            "sequence discontinuity: expected {next}, found {:?}",
                            entry.seq
                        ));
                        break;
                    }
                    if entry.prev_hash.as_deref() != Some(&report.root_hash) {
                        report.error = Some(format!("hash-chain break at seq {next}"));
                        break;
                    }
                    let hash = compute_frame_hash(next, &report.root_hash, &payload);
                    if entry.hash.as_deref() != Some(&hash) {
                        report.error =
                            Some(format!("cryptographic frame hash mismatch at seq {next}"));
                        break;
                    }
                    hash
                }
                version => {
                    report.error = Some(format!("unsupported version {version}"));
                    break;
                }
            };
            // Validate event shape during verification as well as replay.
            if let Err(err) = serde_json::from_value::<Event>(entry.payload) {
                report.error = Some(format!("invalid event at seq {next}: {err}"));
                break;
            }
            report.latest_seq = next;
            report.total_frames += 1;
            report.root_hash = hash;
            for (index, head) in checkpoints.iter().enumerate() {
                if head.latest_seq == next {
                    if head.root_hash != report.root_hash {
                        report.error =
                            Some(format!("ledger checkpoint hash mismatch at seq {next}"));
                        break;
                    }
                    matched[index] = true;
                }
            }
            if report.error.is_some() {
                break;
            }
            offset += line.len() as u64;
        }
        if report.error.is_some() {
            break;
        }
    }
    if report.error.is_none() && matched.iter().any(|matched| !matched) {
        report.error = Some("journal rollback: committed checkpoint is missing".into());
    }
    if report.error.is_some() {
        report.verified = false;
        report.tampering_detected = true;
    } else if let Some(offset) = truncate_at {
        let file = private_file(&root.join("journal.jsonl"), false)?;
        file.set_len(offset)?;
        file.sync_all()?;
    }
    Ok(report)
}

struct CachedVector {
    values: Vec<f32>,
    norm: f32,
}

impl CachedVector {
    fn from_artifact(artifact: &Artifact) -> Option<Self> {
        let embedding = artifact.data.get("embedding").or_else(|| artifact.data.get("vector"))?.as_array()?;
        if embedding.is_empty() { return None; }
        let values: Vec<f32> = embedding.iter().map(|value| value.as_f64().map(|n| n as f32)).collect::<Option<_>>()?;
        let norm = values.iter().map(|value| value * value).sum::<f32>();
        Some(Self { values, norm })
    }
}

pub struct Store {
    root: PathBuf,
    external_checkpoint: Option<PathBuf>,
    _checkpoint_lock: Option<File>,
    _lock: File,
    journal: File,
    artifacts: BTreeMap<String, Arc<Artifact>>,
    collection_index: HashMap<String, HashMap<String, Vec<Arc<Artifact>>>>,
    vector_index: HashMap<String, CachedVector>,
    conversations: BTreeMap<String, Conversation>,
    idempotency: BTreeMap<String, (String, String)>,
    users: BTreeMap<String, UserRecord>,
    sessions: BTreeMap<String, UserSession>,
    buckets: BTreeMap<(String, String), Bucket>,
    files: BTreeMap<String, StoredFile>,
    event_workers: BTreeMap<String, EventWorker>,
    demo_event: Option<Value>,
    webhooks: BTreeMap<String, Webhook>,
    pub healthy: bool,
    pub last_seq: u64,
    pub last_hash: String,
}

impl Store {
    pub fn open(root: &Path) -> io::Result<Self> {
        Self::open_with_checkpoint(root, None)
    }

    pub fn open_with_checkpoint(root: &Path, checkpoint: Option<&Path>) -> io::Result<Self> {
        secure_dir(root)?;
        let external_checkpoint = checkpoint
            .map(|path| {
                let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
                let resolved_parent = parent.canonicalize()?;
                if resolved_parent.starts_with(root.canonicalize()?) {
                    return Err(io::Error::other("external ledger checkpoint must be outside the data directory"));
                }
                let filename = path.file_name().ok_or_else(|| io::Error::other("ledger checkpoint requires a filename"))?;
                Ok(resolved_parent.join(filename))
            })
            .transpose()?;
        secure_dir(&root.join("artifacts"))?;
        let lock = private_file(&root.join("server.lock"), false)?;
        let mut locked = lock.try_lock();
        if locked.is_err() {
            for _ in 0..5 {
                std::thread::sleep(std::time::Duration::from_millis(10));
                locked = lock.try_lock();
                if locked.is_ok() {
                    break;
                }
            }
        }
        locked.map_err(|_| io::Error::other("another server owns this data directory"))?;
        let checkpoint_lock = external_checkpoint
            .as_ref()
            .map(|path| {
                let mut name = path.file_name().unwrap().to_os_string();
                name.push(".lock");
                let file = private_file(&path.with_file_name(name), false)?;
                file.try_lock().map_err(|_| io::Error::other("another server owns this ledger checkpoint"))?;
                Ok::<File, io::Error>(file)
            })
            .transpose()?;
        secure_dir(&root.join("storage"))?;
        secure_dir(&root.join("uploads"))?;
        let journal = private_file(&root.join("journal.jsonl"), false)?;
        if journal.metadata()?.len() > MAX_JOURNAL_BYTES {
            return Err(io::Error::other(
                "journal exceeds storage limit",
            ));
        }
        sync_dir(root)?;
        let mut store = Self {
            root: root.to_path_buf(),
            external_checkpoint,
            _checkpoint_lock: checkpoint_lock,
            _lock: lock,
            journal,
            artifacts: BTreeMap::new(),
            collection_index: HashMap::new(),
            vector_index: HashMap::new(),
            conversations: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            users: BTreeMap::new(),
            sessions: BTreeMap::new(),
            buckets: BTreeMap::new(),
            files: BTreeMap::new(),
            event_workers: BTreeMap::new(),
            demo_event: None,
            webhooks: BTreeMap::new(),
            healthy: true,
            last_seq: 0,
            last_hash: GENESIS_HASH.to_string(),
        };
        let report = scan_ledger(root, &store.checkpoints()?, true)?;
        if !report.verified {
            return Err(io::Error::other(report.error.unwrap_or_else(|| "invalid journal".into())));
        }
        let mut reader = BufReader::new(store.journal.try_clone()?);
        loop {
            let mut line = Vec::new();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            let entry: Entry = serde_json::from_slice(&line).map_err(io::Error::other)?;
            store.apply(serde_json::from_value(entry.payload).map_err(io::Error::other)?);
        }
        store.last_seq = report.latest_seq;
        store.last_hash = report.root_hash;
        store.journal.seek(SeekFrom::End(0))?;
        store.persist_head()?;
        for artifact in store.artifacts.values() {
            store.project(artifact)?;
        }
        for entry in fs::read_dir(root.join("artifacts"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(id) = name.strip_suffix(".toon")
                && id.starts_with("art_")
                && !store.artifacts.contains_key(id)
            {
                if !entry.file_type()?.is_file() {
                    return Err(io::Error::other("invalid artifact projection"));
                }
                fs::remove_file(entry.path())?;
            }
        }
        for entry in fs::read_dir(root.join("uploads"))? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
        let mut verified = BTreeSet::new();
        for file in store.files.values() {
            let path = store.blob_path(file)?;
            if verified.insert(path.clone()) {
                let mut input = File::open(path)?;
                let mut hasher = Sha256::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let size = input.read(&mut buffer)?;
                    if size == 0 {
                        break;
                    }
                    hasher.update(&buffer[..size]);
                }
                if hex(&hasher.finalize()) != file.sha256 {
                    return Err(io::Error::other("blob checksum mismatch"));
                }
            }
        }
        // Reclaim a blob written before a crashed journal commit, or left by a failed deletion.
        for scope in fs::read_dir(root.join("storage"))? {
            let scope = scope?;
            let name = scope.file_name().to_string_lossy().into_owned();
            if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            if !scope.file_type()?.is_dir() {
                return Err(io::Error::other("invalid storage directory"));
            }
            for bucket in fs::read_dir(scope.path())? {
                let bucket = bucket?;
                if !valid_bucket(&bucket.file_name().to_string_lossy()) {
                    continue;
                }
                if !bucket.file_type()?.is_dir() {
                    return Err(io::Error::other("invalid bucket directory"));
                }
                for blob in fs::read_dir(bucket.path())? {
                    let blob = blob?;
                    let name = blob.file_name().to_string_lossy().into_owned();
                    if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                        continue;
                    }
                    if !blob.file_type()?.is_file() {
                        return Err(io::Error::other("invalid blob file"));
                    }
                    if !verified.contains(&blob.path()) {
                        fs::remove_file(blob.path())?;
                    }
                }
            }
        }
        Ok(store)
    }

    fn apply(&mut self, event: Event) {
        match event {
            Event::SnapshotReset { idempotency } => {
                self.artifacts.clear();
                self.collection_index.clear();
                self.vector_index.clear();
                self.conversations.clear();
                self.idempotency = idempotency;
                self.users.clear();
                self.sessions.clear();
                self.buckets.clear();
                self.files.clear();
                self.event_workers.clear();
                self.demo_event = None;
                self.webhooks.clear();
            }
            Event::Batch { events } => {
                for event in events { self.apply(event); }
            }
            Event::Artifact {
                artifact,
                idempotency,
                fingerprint,
            } => {
                if let Some(key) = idempotency {
                    self.idempotency
                        .insert(key, (fingerprint, artifact.id.clone()));
                }
                if let Some(vector) = CachedVector::from_artifact(&artifact) {
                    self.vector_index.insert(artifact.id.clone(), vector);
                } else {
                    self.vector_index.remove(&artifact.id);
                }
                let artifact = Arc::new(artifact);
                if let Some(old) = self.artifacts.insert(artifact.id.clone(), Arc::clone(&artifact)) {
                    if let Some(kinds) = self.collection_index.get_mut(&old.workspace_id) {
                        if let Some(list) = kinds.get_mut(&old.kind) {
                            list.retain(|a| a.id != old.id);
                        }
                    }
                }
                self.collection_index.entry(artifact.workspace_id.clone()).or_default()
                    .entry(artifact.kind.clone()).or_default().push(Arc::clone(&artifact));
            }
            Event::ArtifactDeleted { id } => {
                self.vector_index.remove(&id);
                if let Some(old) = self.artifacts.remove(&id) {
                    if let Some(kinds) = self.collection_index.get_mut(&old.workspace_id) {
                        if let Some(list) = kinds.get_mut(&old.kind) {
                            list.retain(|a| a.id != id);
                        }
                    }
                }
                self.idempotency.retain(|_, (_, art_id)| art_id != &id);
            }
            Event::Conversation { conversation } => {
                self.conversations
                    .insert(conversation.id.clone(), conversation);
            }
            Event::UserCreated { user } => {
                self.users.insert(user.id.clone(), user);
            }
            Event::UserDeleted { id } => {
                self.users.remove(&id);
                self.sessions.retain(|_, session| session.user_id != id);
                self.conversations
                    .retain(|_, conversation| conversation.owner != format!("user:{id}"));
                let file_ids: Vec<String> = self
                    .files
                    .values()
                    .filter(|file| file.user_id.as_deref() == Some(id.as_str()))
                    .map(|file| file.id.clone())
                    .collect();
                for file_id in file_ids {
                    if let Some(file) = self.files.remove(&file_id) {
                        if let Some(old) = self.artifacts.remove(&file.artifact_id) {
                            self.vector_index.remove(&file.artifact_id);
                            if let Some(kinds) = self.collection_index.get_mut(&old.workspace_id) {
                                if let Some(list) = kinds.get_mut(&old.kind) {
                                    list.retain(|a| a.id != file.artifact_id);
                                }
                            }
                        }
                    }
                }
            }
            Event::SessionCreated { session } => {
                self.sessions.insert(session.token_sha256.clone(), session);
            }
            Event::SessionRevoked { token_sha256 } => {
                self.sessions.remove(&token_sha256);
            }
            Event::BucketCreated { bucket } => {
                self.buckets
                    .insert((bucket.workspace_id.clone(), bucket.name.clone()), bucket);
            }
            Event::FileUploaded { file, artifact } => {
                if let Some(vector) = CachedVector::from_artifact(&artifact) {
                    self.vector_index.insert(artifact.id.clone(), vector);
                } else {
                    self.vector_index.remove(&artifact.id);
                }
                let artifact = Arc::new(artifact);
                if let Some(old) = self.artifacts.insert(artifact.id.clone(), Arc::clone(&artifact)) {
                    if let Some(kinds) = self.collection_index.get_mut(&old.workspace_id) {
                        if let Some(list) = kinds.get_mut(&old.kind) {
                            list.retain(|a| a.id != old.id);
                        }
                    }
                }
                self.collection_index.entry(artifact.workspace_id.clone()).or_default()
                    .entry(artifact.kind.clone()).or_default().push(Arc::clone(&artifact));
                self.files.insert(file.id.clone(), file);
            }
            Event::FileDeleted { id } => {
                if let Some(file) = self.files.remove(&id) {
                    if let Some(old) = self.artifacts.remove(&file.artifact_id) {
                        self.vector_index.remove(&file.artifact_id);
                        if let Some(kinds) = self.collection_index.get_mut(&old.workspace_id) {
                            if let Some(list) = kinds.get_mut(&old.kind) {
                                list.retain(|a| a.id != file.artifact_id);
                            }
                        }
                    }
                }
            }
            Event::WorkerCreated { worker } => {
                self.event_workers.insert(worker.id.clone(), worker);
            }
            Event::WorkerDeleted { id } => {
                self.event_workers.remove(&id);
            }
            Event::DemoSeeded { payload: event } => {
                self.demo_event = Some(event);
            }
            Event::WebhookCreated { webhook } => {
                self.webhooks.insert(webhook.id.clone(), webhook);
            }
            Event::WebhookDeleted { id } => {
                self.webhooks.remove(&id);
            }
        }
    }

    fn commit(&mut self, event: Event) -> io::Result<()> {
        if !self.healthy {
            return Err(io::Error::other(
                "storage unavailable after write failure; restart for recovery",
            ));
        }
        let payload = serde_json::to_value(&event).map_err(io::Error::other)?;
        // Keep the canonical Value serialization used by replay, but reuse its
        // bytes in the frame instead of serializing the entire payload twice.
        let payload = serde_json::to_vec(&payload).map_err(io::Error::other)?;
        let next_seq = self.last_seq.checked_add(1).ok_or_else(|| io::Error::other("ledger sequence exhausted"))?;
        let prev_hash = self.last_hash.clone();
        let hash = compute_frame_hash(next_seq, &prev_hash, &payload);
        let mut frame = Vec::with_capacity(payload.len() + 180);
        frame.extend_from_slice(b"{\"version\":2,\"seq\":");
        frame.extend_from_slice(next_seq.to_string().as_bytes());
        frame.extend_from_slice(b",\"prev_hash\":\"");
        frame.extend_from_slice(prev_hash.as_bytes());
        frame.extend_from_slice(b"\",\"payload\":");
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(b",\"hash\":\"");
        frame.extend_from_slice(hash.as_bytes());
        frame.extend_from_slice(b"\"}\n");
        if self.journal.metadata()?.len() + frame.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::other(
                "initial storage limit reached; export before continuing",
            ));
        }
        if let Err(error) = self
            .journal
            .write_all(&frame)
            .and_then(|_| self.journal.sync_data())
        {
            self.healthy = false;
            return Err(error);
        }
        self.last_seq = next_seq;
        self.last_hash = hash;
        if let Err(error) = self.persist_head() {
            self.healthy = false;
            return Err(error);
        }
        self.apply(event);
        Ok(())
    }

    fn project(&self, artifact: &Artifact) -> io::Result<()> {
        self.write_projection(artifact, true)
    }

    fn write_projection(&self, artifact: &Artifact, durable: bool) -> io::Result<()> {
        let destination = self.root.join("artifacts").join(format!("{}.toon", artifact.id));
        let output = crate::toon::encode(&serde_json::to_value(artifact).map_err(io::Error::other)?);
        if durable {
            if destination.exists() && fs::read(&destination)? == output.as_bytes() {
                return Ok(());
            }
            let temporary = self.root.join("artifacts").join(format!("{}.tmp", new_id("projection")));
            let mut file = private_file(&temporary, true)?;
            file.write_all(output.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            sync_dir(&self.root.join("artifacts"))?;
        } else {
            fs::write(&destination, output.as_bytes())?;
        }
        Ok(())
    }

    fn reproject(&self, artifact: &Artifact) -> io::Result<()> {
        let destination = self
            .root
            .join("artifacts")
            .join(format!("{}.toon", artifact.id));
        let temporary = self
            .root
            .join("artifacts")
            .join(format!("{}.tmp", new_id("projection")));
        let mut file = private_file(&temporary, true)?;
        let output =
            crate::toon::encode(&serde_json::to_value(artifact).map_err(io::Error::other)?);
        file.write_all(output.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        sync_dir(&self.root.join("artifacts"))
    }

    pub fn create(
        &mut self,
        workspace: &str,
        kind: String,
        mut data: Map<String, Value>,
        idempotency: Option<String>,
    ) -> io::Result<Artifact> {
        let fingerprint = hex(&Sha256::digest(
            serde_json::to_vec(&(&kind, &data)).map_err(io::Error::other)?,
        ));
        if let Some((old_fingerprint, id)) = idempotency
            .as_ref()
            .and_then(|key| self.idempotency.get(key))
        {
            if old_fingerprint != &fingerprint {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "idempotency key reused with different data",
                ));
            }
            return Ok(self.artifacts[id].as_ref().clone());
        }
        if serde_json::to_vec(&data).map_err(io::Error::other)?.len() > MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "artifact data exceeds 16 MiB",
            ));
        }
        let now_dt = Utc::now();
        let now = now_dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        
        // Support TTL auto-calculation: if ttl (seconds) is provided, compute expires_at
        if let Some(ttl_sec) = data.get("ttl").and_then(|v| v.as_i64()) {
            if ttl_sec > 0 && !data.contains_key("expires_at") {
                let expires_at = (now_dt + chrono::Duration::seconds(ttl_sec)).to_rfc3339();
                data.insert("expires_at".into(), Value::String(expires_at));
            }
        }

        let artifact = Artifact {
            id: new_id("art"),
            workspace_id: workspace.to_owned(),
            kind,
            data,
            revision: 1,
            created_at: now.clone(),
            updated_at: now,
        };
        self.commit(Event::Artifact {
            artifact: artifact.clone(),
            idempotency,
            fingerprint,
        })?;
        if self.project(&artifact).is_err() {
            eprintln!(
                "TOON projection pending; the committed artifact will be projected on restart"
            );
        }
        Ok(artifact)
    }

    pub fn is_expired(artifact: &Artifact, now: &str) -> bool {
        if let Some(exp) = artifact.data.get("expires_at").and_then(|v| v.as_str()) {
            return exp <= now;
        }
        false
    }

    pub fn get(&self, workspace: &str, id: &str) -> Option<Artifact> {
        let now = Utc::now().to_rfc3339();
        self.artifacts
            .get(id)
            .filter(|a| a.workspace_id == workspace && !Self::is_expired(a, &now))
            .map(|a| a.as_ref().clone())
    }

    pub fn is_record_visible(
        &self,
        artifact: &Artifact,
        user_id: Option<&str>,
        owned_files: Option<&BTreeMap<&str, &str>>,
    ) -> bool {
        if artifact.kind == "files" {
            let file_owner = if let Some(owned_files) = owned_files {
                owned_files.get(artifact.id.as_str()).copied()
            } else {
                self.files
                    .values()
                    .find(|f| f.artifact_id == artifact.id)
                    .and_then(|f| f.user_id.as_deref())
            };

            if let Some(owner) = file_owner {
                return Some(owner) == user_id;
            }
        }

        let Some(user_id) = user_id else {
            return true;
        };

        // Explicitly public record
        if artifact.data.get("is_public").and_then(Value::as_bool) == Some(true) {
            return true;
        }

        // Record owned by user
        if let Some(owner) = artifact
            .data
            .get("owner_id")
            .or_else(|| artifact.data.get("user_id"))
            .and_then(Value::as_str)
        {
            return owner == user_id;
        }

        // Records without an explicit owner or not in files collection are shared across workspace
        true
    }

    pub fn can_mutate_record(&self, artifact: &Artifact, user_id: Option<&str>) -> bool {
        let Some(user_id) = user_id else {
            return true;
        };

        if let Some(file) = self.files.values().find(|f| f.artifact_id == artifact.id) {
            return file.user_id.as_deref() == Some(user_id);
        }

        if let Some(owner) = artifact
            .data
            .get("owner_id")
            .or_else(|| artifact.data.get("user_id"))
            .and_then(Value::as_str)
        {
            return owner == user_id;
        }

        // If an app user tries to mutate a record they do not own
        false
    }

    pub fn get_visible(
        &self,
        workspace: &str,
        id: &str,
        user_id: Option<&str>,
    ) -> Option<Artifact> {
        self.get(workspace, id).filter(|artifact| {
            self.is_record_visible(artifact, user_id, None)
        })
    }

    pub fn update(
        &mut self,
        workspace: &str,
        id: &str,
        data_updates: Map<String, Value>,
        merge: bool,
        user_id: Option<&str>,
    ) -> io::Result<Option<Artifact>> {
        let mut current = match self.get_visible(workspace, id, user_id) {
            Some(a) => a,
            None => return Ok(None),
        };
        if !self.can_mutate_record(&current, user_id) {
            return Ok(None);
        }

        let mut new_data = if merge {
            let mut merged = current.data.clone();
            for (k, v) in data_updates {
                merged.insert(k, v);
            }
            merged
        } else {
            data_updates
        };

        let now_dt = Utc::now();
        let now = now_dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);

        if let Some(ttl_sec) = new_data.get("ttl").and_then(|v| v.as_i64()) {
            if ttl_sec > 0 {
                let expires_at = (now_dt + chrono::Duration::seconds(ttl_sec)).to_rfc3339();
                new_data.insert("expires_at".into(), Value::String(expires_at));
            }
        }

        if serde_json::to_vec(&new_data).map_err(io::Error::other)?.len() > MAX_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "artifact data exceeds 16 MiB",
            ));
        }

        current.data = new_data;
        current.revision += 1;
        current.updated_at = now;

        let fingerprint = hex(&Sha256::digest(
            serde_json::to_vec(&(&current.kind, &current.data)).map_err(io::Error::other)?,
        ));

        self.commit(Event::Artifact {
            artifact: current.clone(),
            idempotency: None,
            fingerprint,
        })?;

        let _ = self.reproject(&current);
        Ok(Some(current))
    }

    pub fn delete(
        &mut self,
        workspace: &str,
        id: &str,
        user_id: Option<&str>,
    ) -> io::Result<bool> {
        let current = match self.get_visible(workspace, id, user_id) {
            Some(a) => a,
            None => return Ok(false),
        };
        if !self.can_mutate_record(&current, user_id) {
            return Ok(false);
        }
        self.commit(Event::ArtifactDeleted {
            id: current.id.clone(),
        })?;
        let toon_path = self.root.join("artifacts").join(format!("{}.toon", current.id));
        let _ = fs::remove_file(toon_path);
        Ok(true)
    }

    pub fn bulk_insert(
        &mut self,
        workspace: &str,
        default_kind: Option<String>,
        items: Vec<(Option<String>, Map<String, Value>)>,
    ) -> io::Result<Vec<Artifact>> {
        let mut results = Vec::with_capacity(items.len());
        let mut events = Vec::with_capacity(items.len());
        let now = Utc::now();
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        // Validate and stage everything before writing the single commit frame.
        for (item_kind, mut data) in items {
            let kind = item_kind.or_else(|| default_kind.clone()).unwrap_or_else(|| "records".into());
            if kind.trim().is_empty() || kind.len() > 128 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid collection name"));
            }
            if let Some(ttl) = data.get("ttl").and_then(Value::as_i64).filter(|ttl| *ttl > 0) {
                if !data.contains_key("expires_at") {
                    let expires = chrono::Duration::try_seconds(ttl).and_then(|ttl| now.checked_add_signed(ttl))
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TTL is out of range"))?;
                    data.insert("expires_at".into(), Value::String(expires.to_rfc3339()));
                }
            }
            let artifact = Artifact { id: new_id("art"), workspace_id: workspace.into(), kind, data,
                revision: 1, created_at: timestamp.clone(), updated_at: timestamp.clone() };
            events.push(Event::Artifact { artifact: artifact.clone(), idempotency: None, fingerprint: String::new() });
            results.push(artifact);
        }
        if !events.is_empty() {
            self.commit(Event::Batch { events })?;
            if results.len() <= 64 {
                let artifacts: Vec<Arc<Artifact>> = results.iter()
                    .filter_map(|artifact| self.artifacts.get(&artifact.id).cloned()).collect();
                let root = self.root.clone();
                for artifact in &artifacts {
                    let destination = root.join("artifacts").join(format!("{}.toon", artifact.id));
                    if let Ok(val) = serde_json::to_value(artifact) {
                        let output = crate::toon::encode(&val);
                        let _ = fs::write(&destination, output.as_bytes());
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn bulk_update(
        &mut self,
        workspace: &str,
        items: Vec<(String, Map<String, Value>)>,
        merge: bool,
        user_id: Option<&str>,
    ) -> io::Result<Vec<Artifact>> {
        let mut results = Vec::with_capacity(items.len());
        for (id, data) in items {
            if let Some(artifact) = self.update(workspace, &id, data, merge, user_id)? {
                results.push(artifact);
            }
        }
        Ok(results)
    }

    pub fn bulk_delete(
        &mut self,
        workspace: &str,
        ids: Vec<String>,
        user_id: Option<&str>,
    ) -> io::Result<Vec<String>> {
        let mut deleted_ids = Vec::with_capacity(ids.len());
        for id in ids {
            if self.delete(workspace, &id, user_id)? {
                deleted_ids.push(id);
            }
        }
        Ok(deleted_ids)
    }

    pub fn single_workspace(&self) -> io::Result<Option<String>> {
        let scopes: BTreeSet<_> = self
            .artifacts
            .values()
            .map(|a| a.workspace_id.clone())
            .chain(self.users.values().map(|u| u.workspace_id.clone()))
            .chain(self.buckets.values().map(|b| b.workspace_id.clone()))
            .chain(self.conversations.values().map(|c| c.workspace_id.clone()))
            .collect();
        if scopes.len() > 1 {
            return Err(io::Error::other(
                "Stored data contains multiple workspaces. Export or migrate it before using the single-workspace server; no data was changed.",
            ));
        }
        Ok(scopes.into_iter().next())
    }

    pub fn collections(&self, workspace: &str) -> Vec<String> {
        let mut list: Vec<String> = self
            .collection_index
            .get(workspace)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        list.sort();
        list
    }

    pub fn list(&self, workspace: &str, kind: Option<&str>) -> Vec<Artifact> {
        let now = Utc::now().to_rfc3339();
        let mut records: Vec<_> = if let Some(k) = kind {
            if let Some(arts) = self.collection_index.get(workspace).and_then(|kinds| kinds.get(k)) {
                arts.iter()
                    .filter(|a| !Self::is_expired(a, &now))
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            self.artifacts.values()
                .filter(|a| a.workspace_id == workspace && !Self::is_expired(a, &now))
                .collect()
        };
        records.sort_unstable_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
        records.into_iter().map(|a| a.as_ref().clone()).collect()
    }

    pub fn snapshot_visible(&self, workspace: &str, kind: Option<&str>, user_id: Option<&str>) -> Vec<Arc<Artifact>> {
        let mut records = self.visible_refs(workspace, kind, user_id);
        records.sort_unstable_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
        records.into_iter().cloned().collect()
    }

    pub fn visible_refs(&self, workspace: &str, kind: Option<&str>, user_id: Option<&str>) -> Vec<&Arc<Artifact>> {
        let now = Utc::now().to_rfc3339();
        let owned_files: Option<BTreeMap<&str, &str>> = if user_id.is_some() || kind.is_none() || kind == Some("files") {
            Some(self.files.values()
                .filter_map(|f| f.user_id.as_deref().map(|owner| (f.artifact_id.as_str(), owner))).collect())
        } else {
            None
        };
        if let Some(k) = kind {
            if let Some(arts) = self.collection_index.get(workspace).and_then(|kinds| kinds.get(k)) {
                arts.iter()
                    .filter(|a| !Self::is_expired(a, &now) && self.is_record_visible(a, user_id, owned_files.as_ref()))
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            self.artifacts.values().filter(|a| a.workspace_id == workspace
                && !Self::is_expired(a, &now)
                && self.is_record_visible(a, user_id, owned_files.as_ref())).collect()
        }
    }

    pub fn list_visible(&self, workspace: &str, kind: Option<&str>, user_id: Option<&str>) -> Vec<Artifact> {
        self.snapshot_visible(workspace, kind, user_id).into_iter().map(|a| a.as_ref().clone()).collect()
    }

    pub fn page_visible(&self, workspace: &str, kind: Option<&str>, user_id: Option<&str>,
        anchor: Option<(&str, &str)>, offset: usize, limit: usize) -> Option<(Vec<Arc<Artifact>>, usize, bool)> {
        let mut records = self.visible_refs(workspace, kind, user_id);
        let total = records.len();
        if kind.is_none() || anchor.is_some() {
            records.sort_unstable_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
        }
        let start = if let Some((created_at, id)) = anchor {
            records.iter().position(|a| a.created_at == created_at && a.id == id)? + 1
        } else { offset.min(total) };
        let end = start.saturating_add(limit).min(total);
        Some((records[start..end].iter().copied().cloned().collect(), total, end < total))
    }

    pub fn search_vectors(&self, workspace: &str, collection: Option<&str>, query_vector: &[f32],
        top_k: usize, min_score: Option<f32>, user_id: Option<&str>) -> Vec<(Artifact, f32)> {
        let mut scored = Vec::new();
        let query_norm = query_vector.iter().map(|value| value * value).sum::<f32>();
        for record in self.visible_refs(workspace, collection, user_id) {
            let Some(vector) = self.vector_index.get(&record.id) else { continue; };
            if vector.values.len() != query_vector.len() { continue; }
            let norm = query_norm * vector.norm;
            let score = if norm <= 0.0 {
                0.0
            } else {
                let dot = query_vector.iter().zip(&vector.values).map(|(a, b)| a * b).sum::<f32>();
                dot / norm.sqrt()
            };
            if score >= min_score.unwrap_or(0.0) { scored.push((record, score)); }
        }
        // Preserve the previous stable tie order (creation time, then ID).
        let compare = |(a, x): &(&Arc<Artifact>, f32), (b, y): &(&Arc<Artifact>, f32)|
            y.total_cmp(x).then_with(|| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
        if top_k < scored.len() {
            if top_k > 0 { scored.select_nth_unstable_by(top_k, compare); }
            scored.truncate(top_k);
        }
        scored.sort_unstable_by(compare);
        scored.into_iter().map(|(record, score)| (record.as_ref().clone(), score)).collect()
    }

    pub fn conversation(&self, id: &str, owner: &str, workspace: &str) -> Option<Conversation> {
        self.conversations
            .get(id)
            .filter(|c| c.owner == owner && c.workspace_id == workspace)
            .cloned()
    }

    pub fn save_conversation(&mut self, conversation: Conversation) -> io::Result<()> {
        self.commit(Event::Conversation { conversation })
    }

    pub fn find_user(&self, workspace: &str, username: &str) -> Option<UserRecord> {
        self.users
            .values()
            .find(|u| u.workspace_id == workspace && u.username == username)
            .cloned()
    }
    pub fn users(&self, workspace: &str) -> Vec<UserRecord> {
        self.users
            .values()
            .filter(|u| u.workspace_id == workspace)
            .cloned()
            .collect()
    }
    pub fn user_exists(&self, workspace: &str, user_id: &str) -> bool {
        self.users
            .get(user_id)
            .is_some_and(|user| user.workspace_id == workspace)
    }
    pub fn delete_user(&mut self, workspace: &str, user_id: &str) -> io::Result<bool> {
        if !self.user_exists(workspace, user_id) {
            return Ok(false);
        }
        let files: Vec<_> = self
            .files
            .values()
            .filter(|file| file.user_id.as_deref() == Some(user_id))
            .cloned()
            .collect();
        self.commit(Event::UserDeleted {
            id: user_id.to_owned(),
        })?;
        for file in files {
            if !self.files.values().any(|other| {
                other.workspace_id == file.workspace_id
                    && other.bucket == file.bucket
                    && other.sha256 == file.sha256
            }) {
                let _ = fs::remove_file(self.blob_destination(&file)?);
            }
            let _ = fs::remove_file(
                self.root
                    .join("artifacts")
                    .join(format!("{}.toon", file.artifact_id)),
            );
        }
        Ok(true)
    }
    pub fn create_user(
        &mut self,
        workspace: String,
        username: String,
        password_hash: String,
    ) -> io::Result<UserRecord> {
        if self.find_user(&workspace, &username).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "username already registered",
            ));
        }
        if self.users.len() >= 100000 {
            return Err(io::Error::other("user limit reached"));
        }
        let user = UserRecord {
            id: new_id("usr"),
            workspace_id: workspace,
            username,
            password_hash,
            created_at: Utc::now().to_rfc3339(),
        };
        self.commit(Event::UserCreated { user: user.clone() })?;
        Ok(user)
    }
    pub fn create_session(&mut self, session: UserSession) -> io::Result<()> {
        self.sessions
            .retain(|_, s| s.expires_at > Utc::now().timestamp());
        if self.sessions.len() >= 1000000 {
            return Err(io::Error::other("session limit reached"));
        }
        self.commit(Event::SessionCreated { session })
    }
    pub fn session_user(&self, hash: &str) -> Option<UserRecord> {
        let session = self.sessions.get(hash)?;
        if session.expires_at <= Utc::now().timestamp() {
            return None;
        }
        self.users.get(&session.user_id).cloned()
    }
    pub fn revoke_session(&mut self, hash: String) -> io::Result<()> {
        self.commit(Event::SessionRevoked { token_sha256: hash })
    }
    pub fn buckets(&self, workspace: &str) -> Vec<Bucket> {
        self.buckets
            .values()
            .filter(|b| b.workspace_id == workspace)
            .cloned()
            .collect()
    }
    pub fn create_bucket(
        &mut self,
        workspace: &str,
        name: &str,
        owner: &str,
    ) -> io::Result<Bucket> {
        if let Some(bucket) = self.buckets.get(&(workspace.to_owned(), name.to_owned())) {
            return Ok(bucket.clone());
        }
        if self.buckets.len() >= 50000 {
            return Err(io::Error::other("bucket limit reached"));
        }
        let bucket = Bucket {
            workspace_id: workspace.into(),
            name: name.into(),
            created_at: Utc::now().to_rfc3339(),
            created_by: owner.into(),
        };
        self.commit(Event::BucketCreated {
            bucket: bucket.clone(),
        })?;
        Ok(bucket)
    }
    pub fn files(&self, workspace: &str, bucket: &str) -> Vec<StoredFile> {
        self.files
            .values()
            .filter(|f| f.workspace_id == workspace && f.bucket == bucket)
            .cloned()
            .collect()
    }
    pub fn file(&self, workspace: &str, bucket: &str, id: &str) -> Option<StoredFile> {
        self.files
            .get(id)
            .filter(|f| f.workspace_id == workspace && f.bucket == bucket)
            .cloned()
    }
    pub fn event_workers(&self) -> Vec<EventWorker> {
        self.event_workers.values().cloned().collect()
    }
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.journal.metadata()?.len() == 0)
    }
    pub fn demo_event(&self) -> Option<Value> {
        self.demo_event.clone()
    }
    pub fn finish_demo_seed(&mut self, event: Value) -> io::Result<()> {
        self.commit(Event::DemoSeeded { payload: event })
    }
    pub fn event_worker(&self, id: &str) -> Option<EventWorker> {
        self.event_workers.get(id).cloned()
    }
    pub fn update_event_worker(&mut self, id: &str, event_name: Option<String>, source: Option<String>) -> io::Result<Option<EventWorker>> {
        let Some(mut worker) = self.event_worker(id) else { return Ok(None); };
        if let Some(event_name) = event_name { worker.event_name = event_name; }
        if let Some(source) = source { worker.source = source; }
        self.commit(Event::WorkerCreated { worker: worker.clone() })?;
        Ok(Some(worker))
    }
    pub fn webhooks(&self) -> Vec<Webhook> {
        self.webhooks.values().cloned().collect()
    }
    pub fn create_event_worker(&mut self, worker: EventWorker) -> io::Result<()> {
        if self.event_workers.len() >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "event worker limit reached",
            ));
        }
        self.commit(Event::WorkerCreated { worker })
    }
    pub fn delete_event_worker(&mut self, id: &str) -> io::Result<bool> {
        if !self.event_workers.contains_key(id) {
            return Ok(false);
        }
        self.commit(Event::WorkerDeleted { id: id.to_owned() })?;
        Ok(true)
    }
    pub fn create_webhook(&mut self, webhook: Webhook) -> io::Result<()> {
        if self.webhooks.len() >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "webhook limit reached",
            ));
        }
        self.commit(Event::WebhookCreated { webhook })
    }
    pub fn delete_webhook(&mut self, id: &str) -> io::Result<bool> {
        if !self.webhooks.contains_key(id) {
            return Ok(false);
        }
        self.commit(Event::WebhookDeleted { id: id.to_owned() })?;
        Ok(true)
    }
    pub fn begin_upload(&self) -> io::Result<UploadTicket> {
        let path = self.root.join("uploads").join(new_id("upload"));
        let file = private_file(&path, true)?;
        Ok(UploadTicket { path, file })
    }
    fn blob_destination(&self, file: &StoredFile) -> io::Result<PathBuf> {
        if !valid_bucket(&file.bucket)
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(io::Error::other("invalid blob metadata"));
        }
        Ok(self
            .root
            .join("storage")
            .join(hex(&Sha256::digest(file.workspace_id.as_bytes())))
            .join(&file.bucket)
            .join(&file.sha256))
    }
    pub fn blob_path(&self, file: &StoredFile) -> io::Result<PathBuf> {
        let path = self.blob_destination(file)?;
        let meta = fs::symlink_metadata(&path)?;
        if !meta.is_file() || meta.len() != file.size {
            return Err(io::Error::other("blob missing or invalid"));
        }
        Ok(path)
    }
    pub fn finish_upload(
        &mut self,
        mut file: StoredFile,
        ticket: &UploadTicket,
    ) -> io::Result<StoredFile> {
        if file
            .user_id
            .as_deref()
            .is_some_and(|owner| !self.user_exists(&file.workspace_id, owner))
        {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file owner no longer exists",
            ));
        }
        if self.files.len() >= 1000000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file count quota exceeded",
            ));
        }
        let workspace_size: u64 = self
            .files
            .values()
            .filter(|f| f.workspace_id == file.workspace_id)
            .map(|f| f.size)
            .sum();
        let total: u64 = self.files.values().map(|f| f.size).sum();
        if workspace_size + file.size > 100 * 1024 * 1024 * 1024 || total + file.size > 500 * 1024 * 1024 * 1024
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage quota exceeded",
            ));
        }
        self.create_bucket(&file.workspace_id, &file.bucket, &file.created_by)?;
        let destination = self.blob_destination(&file)?;
        secure_dir(destination.parent().unwrap().parent().unwrap())?;
        secure_dir(destination.parent().unwrap())?;
        let new_blob = !destination.exists();
        if !new_blob {
            self.blob_path(&file)?;
        } else {
            fs::rename(&ticket.path, &destination)?;
            sync_dir(destination.parent().unwrap())?;
            sync_dir(destination.parent().unwrap().parent().unwrap())?;
            sync_dir(&self.root.join("storage"))?;
        }
        file.artifact_id = new_id("art");
        let mut data = serde_json::to_value(&file)
            .map_err(io::Error::other)?
            .as_object()
            .unwrap()
            .clone();
        data.remove("workspace_id");
        data.insert("file_id".into(), serde_json::json!(file.id));
        let artifact = Artifact {
            id: file.artifact_id.clone(),
            workspace_id: file.workspace_id.clone(),
            kind: "files".into(),
            data,
            revision: 1,
            created_at: file.created_at.clone(),
            updated_at: file.created_at.clone(),
        };
        if let Err(error) = self.commit(Event::FileUploaded {
            file: file.clone(),
            artifact: artifact.clone(),
        }) {
            // A write/sync failure may still have produced a recoverable commit. Keep
            // its blob until recovery; an early quota/journal rejection has no commit.
            if new_blob && self.healthy {
                let _ = fs::remove_file(&destination);
            }
            return Err(error);
        }
        let _ = self.project(&artifact);
        Ok(file)
    }
    pub fn delete_file(&mut self, file: StoredFile) -> io::Result<()> {
        self.commit(Event::FileDeleted {
            id: file.id.clone(),
        })?;
        if !self.files.values().any(|f| {
            f.workspace_id == file.workspace_id
                && f.bucket == file.bucket
                && f.sha256 == file.sha256
        }) {
            let _ = fs::remove_file(self.blob_destination(&file)?);
        }
        let _ = fs::remove_file(
            self.root
                .join("artifacts")
                .join(format!("{}.toon", file.artifact_id)),
        );
        Ok(())
    }

    pub fn backup_full(&self, destination: &Path) -> io::Result<()> {
        let report = self.verify_ledger()?;
        if !report.verified {
            return Err(io::Error::other(
                report.error.unwrap_or_else(|| "invalid journal".into()),
            ));
        }
        fs::create_dir(destination)?;
        secure_dir(destination)?;
        for source in journal_segments(&self.root)? {
            let target =
                destination.join(source.strip_prefix(&self.root).map_err(io::Error::other)?);
            secure_dir(target.parent().unwrap())?;
            let mut file = private_file(&target, true)?;
            io::copy(&mut File::open(source)?, &mut file)?;
            file.sync_all()?;
            sync_dir(target.parent().unwrap())?;
        }
        write_checkpoint(
            &destination.join("ledger-head.json"),
            &LedgerCheckpoint {
                latest_seq: self.last_seq,
                root_hash: self.last_hash.clone(),
            },
        )?;
        for file in self.files.values() {
            let source = self.blob_path(file)?;
            let target =
                destination.join(source.strip_prefix(&self.root).map_err(io::Error::other)?);
            secure_dir(target.parent().unwrap().parent().unwrap().parent().unwrap())?;
            secure_dir(target.parent().unwrap().parent().unwrap())?;
            secure_dir(target.parent().unwrap())?;
            if !target.exists() {
                let mut output = private_file(&target, true)?;
                io::copy(&mut File::open(source)?, &mut output)?;
                output.sync_all()?;
                sync_dir(target.parent().unwrap())?;
                sync_dir(target.parent().unwrap().parent().unwrap())?;
                sync_dir(target.parent().unwrap().parent().unwrap().parent().unwrap())?;
            }
        }
        sync_dir(destination)
    }

    pub fn backup(&self, destination: &Path) -> io::Result<()> {
        let report = self.verify_ledger()?;
        if !report.verified {
            return Err(io::Error::other(
                report.error.unwrap_or_else(|| "invalid journal".into()),
            ));
        }
        let segments = journal_segments(&self.root)?;
        let bytes = segments
            .iter()
            .try_fold(0u64, |total, path| -> io::Result<u64> {
                total
                    .checked_add(fs::metadata(path)?.len())
                    .ok_or_else(|| io::Error::other("backup size overflow"))
            })?;
        if bytes > MAX_JOURNAL_BYTES {
            return Err(io::Error::other(
                "flattened journal backup exceeds 64 GiB; use --include-files to preserve segments",
            ));
        }
        let mut target = private_file(destination, true)?;
        // Flatten segments into a standalone journal. SnapshotReset frames make
        // replay discard old state while preserving the complete audit chain.
        for path in segments {
            io::copy(&mut File::open(path)?, &mut target)?;
        }
        target.sync_all()?;
        if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(())
    }

    pub fn vacuum(&mut self) -> io::Result<VacuumStats> {
        if !self.healthy {
            return Err(io::Error::other(
                "storage unavailable after write failure; restart for recovery",
            ));
        }
        let report = self.verify_ledger()?;
        if !report.verified {
            return Err(io::Error::other(
                report.error.unwrap_or_else(|| "invalid journal".into()),
            ));
        }
        let initial_bytes = self.journal.metadata()?.len();
        let now = Utc::now();
        let expired_ids: BTreeSet<String> = self
            .artifacts
            .values()
            .filter_map(|artifact| {
                artifact
                    .data
                    .get("expires_at")
                    .and_then(Value::as_str)
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .filter(|expiry| *expiry <= now)
                    .map(|_| artifact.id.clone())
            })
            .collect();
        let idempotency = self
            .idempotency
            .iter()
            .filter(|(_, (_, id))| self.artifacts.contains_key(id) && !expired_ids.contains(id))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let mut events = vec![Event::SnapshotReset { idempotency }];
        events.extend(
            self.users
                .values()
                .map(|user| Event::UserCreated { user: user.clone() }),
        );
        events.extend(
            self.sessions
                .values()
                .filter(|session| session.expires_at > now.timestamp())
                .map(|session| Event::SessionCreated {
                    session: session.clone(),
                }),
        );
        events.extend(self.buckets.values().map(|bucket| Event::BucketCreated {
            bucket: bucket.clone(),
        }));
        let file_artifact_ids: BTreeSet<&str> = self
            .files
            .values()
            .map(|file| file.artifact_id.as_str())
            .collect();
        for artifact in self.artifacts.values() {
            if !expired_ids.contains(&artifact.id)
                && !file_artifact_ids.contains(artifact.id.as_str())
            {
                let fingerprint = hex(&Sha256::digest(
                    serde_json::to_vec(&(&artifact.kind, &artifact.data))
                        .map_err(io::Error::other)?,
                ));
                events.push(Event::Artifact {
                    artifact: artifact.as_ref().clone(),
                    idempotency: None,
                    fingerprint,
                });
            }
        }
        for file in self.files.values() {
            if let Some(artifact) = self
                .artifacts
                .get(&file.artifact_id)
                .filter(|a| !expired_ids.contains(&a.id))
            {
                events.push(Event::FileUploaded {
                    file: file.clone(),
                    artifact: artifact.as_ref().clone(),
                });
            }
        }
        events.extend(
            self.event_workers
                .values()
                .map(|worker| Event::WorkerCreated {
                    worker: worker.clone(),
                }),
        );
        if let Some(payload) = &self.demo_event {
            events.push(Event::DemoSeeded {
                payload: payload.clone(),
            });
        }
        events.extend(self.webhooks.values().map(|webhook| Event::WebhookCreated {
            webhook: webhook.clone(),
        }));
        events.extend(
            self.conversations
                .values()
                .map(|conversation| Event::Conversation {
                    conversation: conversation.clone(),
                }),
        );

        // Stage the snapshot without changing live state. The old head is the
        // authenticated predecessor, and every snapshot frame consumes a new seq.
        let compact_path = self.root.join(new_id("journal.compact"));
        let mut cur_seq = self.last_seq;
        let mut cur_hash = self.last_hash.clone();
        let staged = (|| {
            let mut file = private_file(&compact_path, true)?;
            for event in &events {
                cur_seq = cur_seq
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("ledger sequence exhausted"))?;
                cur_hash = Self::write_entry_v2(&mut file, cur_seq, &cur_hash, event)?;
            }
            if file.metadata()?.len() > MAX_JOURNAL_BYTES {
                return Err(io::Error::other("compacted journal exceeds storage limit"));
            }
            file.sync_all()?;
            Ok(file.metadata()?.len())
        })();
        let compacted_bytes = match staged {
            Ok(bytes) => bytes,
            Err(err) => {
                let _ = fs::remove_file(&compact_path);
                return Err(err);
            }
        };

        // Publish a complete immutable archive before replacing the active file.
        // A crash before the swap leaves an unused archive; after the swap the
        // first snapshot frame resolves it by prev_hash. No multi-file manifest
        // update is required for recovery.
        if self.last_seq > 0 {
            let history = self.root.join("journal-history");
            let temporary = history.join(new_id("archive"));
            let archive_result = (|| {
                secure_dir(&history)?;
                let mut file = private_file(&temporary, true)?;
                io::copy(&mut File::open(self.root.join("journal.jsonl"))?, &mut file)?;
                file.sync_all()?;
                fs::rename(
                    &temporary,
                    history.join(format!("{}.jsonl", self.last_hash)),
                )?;
                sync_dir(&history)?;
                sync_dir(&self.root)
            })();
            if let Err(err) = archive_result {
                let _ = fs::remove_file(&temporary);
                let _ = fs::remove_file(&compact_path);
                return Err(err);
            }
        }
        let journal_path = self.root.join("journal.jsonl");
        if let Err(err) = fs::rename(&compact_path, &journal_path) {
            let _ = fs::remove_file(&compact_path);
            return Err(err);
        }
        // Fail closed after the swap until file handle, directory and checkpoints
        // are durable. Recovery accepts the old checkpoint as a verified prefix.
        self.healthy = false;
        sync_dir(&self.root)?;
        self.journal = private_file(&journal_path, false)?;
        self.journal.seek(SeekFrom::End(0))?;
        self.last_seq = cur_seq;
        self.last_hash = cur_hash;
        self.persist_head()?;
        for event in events {
            self.apply(event);
        }
        self.healthy = true;
        for id in &expired_ids {
            let _ = fs::remove_file(self.root.join("artifacts").join(format!("{id}.toon")));
        }
        Ok(VacuumStats {
            initial_bytes,
            compacted_bytes,
            active_artifacts: self.artifacts.len(),
            active_users: self.users.len(),
            active_files: self.files.len(),
            purged_expired: expired_ids.len(),
        })
    }

    fn write_entry_v2(
        file: &mut File,
        seq: u64,
        prev_hash: &str,
        event: &Event,
    ) -> io::Result<String> {
        let payload = serde_json::to_value(&event).map_err(io::Error::other)?;
        let payload = serde_json::to_vec(&payload).map_err(io::Error::other)?;
        let hash = compute_frame_hash(seq, prev_hash, &payload);
        let mut frame = Vec::with_capacity(payload.len() + 180);
        frame.extend_from_slice(b"{\"version\":2,\"seq\":");
        frame.extend_from_slice(seq.to_string().as_bytes());
        frame.extend_from_slice(b",\"prev_hash\":\"");
        frame.extend_from_slice(prev_hash.as_bytes());
        frame.extend_from_slice(b"\",\"payload\":");
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(b",\"hash\":\"");
        frame.extend_from_slice(hash.as_bytes());
        frame.extend_from_slice(b"\"}\n");
        file.write_all(&frame)?;
        Ok(hash)
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn root_hash(&self) -> &str {
        &self.last_hash
    }

    fn checkpoints(&self) -> io::Result<Vec<LedgerCheckpoint>> {
        let mut heads = Vec::new();
        if let Some(head) = read_checkpoint(&self.root.join("ledger-head.json"))? {
            heads.push(head);
        }
        if let Some(path) = &self.external_checkpoint {
            if let Some(head) = read_checkpoint(path)? {
                heads.push(head);
            }
        }
        Ok(heads)
    }

    fn persist_head(&self) -> io::Result<()> {
        let head = LedgerCheckpoint {
            latest_seq: self.last_seq,
            root_hash: self.last_hash.clone(),
        };
        write_checkpoint(&self.root.join("ledger-head.json"), &head)?;
        if let Some(path) = &self.external_checkpoint {
            write_checkpoint(path, &head)?;
        }
        Ok(())
    }

    pub fn verify_ledger(&self) -> io::Result<LedgerReport> {
        let mut checkpoints = self.checkpoints()?;
        checkpoints.push(LedgerCheckpoint {
            latest_seq: self.last_seq,
            root_hash: self.last_hash.clone(),
        });
        let mut report = scan_ledger(&self.root, &checkpoints, false)?;
        if report.verified
            && (report.latest_seq != self.last_seq || report.root_hash != self.last_hash)
        {
            report.verified = false;
            report.tampering_detected = true;
            report.error = Some("journal head differs from live committed head".into());
        }
        Ok(report)
    }

}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VacuumStats {
    pub initial_bytes: u64,
    pub compacted_bytes: u64,
    pub active_artifacts: usize,
    pub active_users: usize,
    pub active_files: usize,
    pub purged_expired: usize,
}
