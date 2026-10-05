use chrono::Utc;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
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
    OsRng.fill_bytes(&mut bytes);
    format!("{prefix}_{}", hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;

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
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
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
    WebhookCreated {
        webhook: Webhook,
    },
    WebhookDeleted {
        id: String,
    },
}

#[derive(Serialize, Deserialize)]
struct Entry {
    version: u32,
    payload: Value,
    sha256: String,
}

pub struct Store {
    root: PathBuf,
    _lock: File,
    journal: File,
    artifacts: BTreeMap<String, Artifact>,
    conversations: BTreeMap<String, Conversation>,
    idempotency: BTreeMap<String, (String, String)>,
    users: BTreeMap<String, UserRecord>,
    sessions: BTreeMap<String, UserSession>,
    buckets: BTreeMap<(String, String), Bucket>,
    files: BTreeMap<String, StoredFile>,
    event_workers: BTreeMap<String, EventWorker>,
    webhooks: BTreeMap<String, Webhook>,
    pub healthy: bool,
}

impl Store {
    pub fn open(root: &Path) -> io::Result<Self> {
        secure_dir(root)?;
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
            _lock: lock,
            journal,
            artifacts: BTreeMap::new(),
            conversations: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            users: BTreeMap::new(),
            sessions: BTreeMap::new(),
            buckets: BTreeMap::new(),
            files: BTreeMap::new(),
            event_workers: BTreeMap::new(),
            webhooks: BTreeMap::new(),
            healthy: true,
        };
        let mut reader = BufReader::new(store.journal.try_clone()?);
        let mut offset = 0u64;
        loop {
            let mut line = Vec::new();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            if line.last() != Some(&b'\n') {
                // Only an incomplete final frame is discarded. Complete invalid frames fail closed.
                store.journal.set_len(offset)?;
                store.journal.sync_all()?;
                break;
            }
            let entry: Entry = serde_json::from_slice(&line).map_err(io::Error::other)?;
            let payload = serde_json::to_vec(&entry.payload).map_err(io::Error::other)?;
            if entry.version != 1 || hex(&Sha256::digest(&payload)) != entry.sha256 {
                return Err(io::Error::other("unsupported or corrupt journal entry"));
            }
            store.apply(serde_json::from_value(entry.payload).map_err(io::Error::other)?);
            offset += line.len() as u64;
        }
        store.journal.seek(SeekFrom::End(0))?;
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
            Event::Artifact {
                artifact,
                idempotency,
                fingerprint,
            } => {
                if let Some(key) = idempotency {
                    self.idempotency
                        .insert(key, (fingerprint, artifact.id.clone()));
                }
                self.artifacts.insert(artifact.id.clone(), artifact);
            }
            Event::ArtifactDeleted { id } => {
                self.artifacts.remove(&id);
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
                        self.artifacts.remove(&file.artifact_id);
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
                self.artifacts.insert(artifact.id.clone(), artifact);
                self.files.insert(file.id.clone(), file);
            }
            Event::FileDeleted { id } => {
                if let Some(file) = self.files.remove(&id) {
                    self.artifacts.remove(&file.artifact_id);
                }
            }
            Event::WorkerCreated { worker } => {
                self.event_workers.insert(worker.id.clone(), worker);
            }
            Event::WorkerDeleted { id } => {
                self.event_workers.remove(&id);
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
        let sha256 = hex(&Sha256::digest(
            serde_json::to_vec(&payload).map_err(io::Error::other)?,
        ));
        let mut frame = serde_json::to_vec(&Entry {
            version: 1,
            payload,
            sha256,
        })
        .map_err(io::Error::other)?;
        frame.push(b'\n');
        if self.journal.metadata()?.len() + frame.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::other(
                "initial storage limit reached; export before continuing",
            ));
        }
        if let Err(error) = self
            .journal
            .write_all(&frame)
            .and_then(|_| self.journal.sync_all())
        {
            self.healthy = false;
            return Err(error);
        }
        self.apply(event);
        Ok(())
    }

    fn project(&self, artifact: &Artifact) -> io::Result<()> {
        let destination = self
            .root
            .join("artifacts")
            .join(format!("{}.toon", artifact.id));
        if destination.exists() {
            return Ok(());
        }
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
            return Ok(self.artifacts[id].clone());
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
            .cloned()
    }

    pub fn is_record_visible(
        &self,
        artifact: &Artifact,
        user_id: Option<&str>,
        owned_files: Option<&BTreeMap<&str, &str>>,
    ) -> bool {
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
        for (item_kind, data) in items {
            let kind = item_kind
                .or_else(|| default_kind.clone())
                .unwrap_or_else(|| "records".to_string());
            let artifact = self.create(workspace, kind, data, None)?;
            results.push(artifact);
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

    pub fn list(&self, workspace: &str, kind: Option<&str>) -> Vec<Artifact> {
        let now = Utc::now().to_rfc3339();
        let mut records: Vec<_> = self
            .artifacts
            .values()
            .filter(|a| a.workspace_id == workspace && kind.is_none_or(|k| a.kind == k) && !Self::is_expired(a, &now))
            .cloned()
            .collect();
        records.sort_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
        records
    }

    pub fn list_visible(
        &self,
        workspace: &str,
        kind: Option<&str>,
        user_id: Option<&str>,
    ) -> Vec<Artifact> {
        let owned_files: BTreeMap<&str, &str> = self
            .files
            .values()
            .filter_map(|file| {
                file.user_id
                    .as_deref()
                    .map(|owner| (file.artifact_id.as_str(), owner))
            })
            .collect();
        self.list(workspace, kind)
            .into_iter()
            .filter(|artifact| {
                self.is_record_visible(artifact, user_id, Some(&owned_files))
            })
            .collect()
    }

    pub fn search_vectors(
        &self,
        workspace: &str,
        collection: Option<&str>,
        query_vector: &[f32],
        top_k: usize,
        min_score: Option<f32>,
        user_id: Option<&str>,
    ) -> Vec<(Artifact, f32)> {
        let min_score = min_score.unwrap_or(0.0);
        let mut scored: Vec<(Artifact, f32)> = Vec::new();
        let records = self.list_visible(workspace, collection, user_id);

        for record in records {
            let embedding = record
                .data
                .get("embedding")
                .or_else(|| record.data.get("vector"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_f64().map(|f| f as f32))
                        .collect::<Vec<f32>>()
                });

            if let Some(vec) = embedding {
                if vec.len() == query_vector.len() {
                    let score = cosine_similarity(query_vector, &vec);
                    if score >= min_score {
                        scored.push((record, score));
                    }
                }
            }
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if scored.len() > top_k {
            scored.truncate(top_k);
        }
        scored
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
        fs::create_dir(destination)?;
        secure_dir(destination)?;
        self.backup(&destination.join("journal.jsonl"))?;
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
        let mut source = File::open(self.root.join("journal.jsonl"))?;
        let mut target = private_file(destination, true)?;
        io::copy(&mut source, &mut target)?;
        target.sync_all()?;
        if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(())
    }

    pub fn vacuum(&mut self) -> io::Result<VacuumStats> {
        let initial_bytes = self.journal.metadata()?.len();
        let compact_path = self.root.join("journal.jsonl.compact");
        let mut new_journal = private_file(&compact_path, true)?;
        let mut purged_expired = 0;
        let now_dt = Utc::now();
        let now_str = now_dt.to_rfc3339();

        // 1. Purge expired sessions
        self.sessions.retain(|_, s| s.expires_at > now_dt.timestamp());

        // 2. Purge expired artifacts
        let expired_ids: Vec<String> = self
            .artifacts
            .values()
            .filter_map(|art| {
                if let Some(exp) = art.data.get("expires_at").and_then(|v| v.as_str()) {
                    if exp <= now_str.as_str() {
                        return Some(art.id.clone());
                    }
                }
                None
            })
            .collect();
        for id in expired_ids {
            self.artifacts.remove(&id);
            purged_expired += 1;
            let _ = fs::remove_file(self.root.join("artifacts").join(format!("{id}.toon")));
        }

        // 3. Write active users
        for user in self.users.values() {
            Self::write_entry(&mut new_journal, Event::UserCreated { user: user.clone() })?;
        }

        // 4. Write active sessions
        for session in self.sessions.values() {
            Self::write_entry(&mut new_journal, Event::SessionCreated { session: session.clone() })?;
        }

        // 5. Write active buckets
        for bucket in self.buckets.values() {
            Self::write_entry(&mut new_journal, Event::BucketCreated { bucket: bucket.clone() })?;
        }

        // 6. Write active non-file artifacts
        let file_artifact_ids: BTreeSet<&str> = self.files.values().map(|f| f.artifact_id.as_str()).collect();
        for artifact in self.artifacts.values() {
            if !file_artifact_ids.contains(artifact.id.as_str()) {
                let fingerprint = hex(&Sha256::digest(
                    serde_json::to_vec(&(&artifact.kind, &artifact.data)).unwrap_or_default(),
                ));
                Self::write_entry(&mut new_journal, Event::Artifact {
                    artifact: artifact.clone(),
                    idempotency: None,
                    fingerprint,
                })?;
            }
        }

        // 7. Write active files + their file artifacts
        for file in self.files.values() {
            if let Some(artifact) = self.artifacts.get(&file.artifact_id) {
                Self::write_entry(&mut new_journal, Event::FileUploaded {
                    file: file.clone(),
                    artifact: artifact.clone(),
                })?;
            }
        }

        // 8. Write event workers & webhooks
        for worker in self.event_workers.values() {
            Self::write_entry(&mut new_journal, Event::WorkerCreated { worker: worker.clone() })?;
        }
        for webhook in self.webhooks.values() {
            Self::write_entry(&mut new_journal, Event::WebhookCreated { webhook: webhook.clone() })?;
        }

        // 9. Write conversations
        for conversation in self.conversations.values() {
            Self::write_entry(&mut new_journal, Event::Conversation { conversation: conversation.clone() })?;
        }

        new_journal.sync_all()?;
        let compacted_bytes = new_journal.metadata()?.len();
        drop(new_journal);

        // Atomic swap
        let journal_path = self.root.join("journal.jsonl");
        fs::rename(&compact_path, &journal_path)?;
        sync_dir(&self.root)?;

        self.journal = private_file(&journal_path, false)?;
        self.journal.seek(SeekFrom::End(0))?;

        Ok(VacuumStats {
            initial_bytes,
            compacted_bytes,
            active_artifacts: self.artifacts.len(),
            active_users: self.users.len(),
            active_files: self.files.len(),
            purged_expired,
        })
    }

    fn write_entry(file: &mut File, event: Event) -> io::Result<()> {
        let payload = serde_json::to_value(&event).map_err(io::Error::other)?;
        let sha256 = hex(&Sha256::digest(
            serde_json::to_vec(&payload).map_err(io::Error::other)?,
        ));
        let mut frame = serde_json::to_vec(&Entry {
            version: 1,
            payload,
            sha256,
        })
        .map_err(io::Error::other)?;
        frame.push(b'\n');
        file.write_all(&frame)
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
