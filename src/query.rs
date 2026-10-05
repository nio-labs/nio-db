use crate::storage::Artifact;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value;
use std::{io, path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Semaphore},
};

const MAX_INPUT: usize = 64 * 1024 * 1024;
const MAX_OUTPUT: usize = 4 * 1024 * 1024;

pub struct AlaSql {
    node: PathBuf,
    helper: PathBuf,
    pub ready: bool,
    capacity: Semaphore,
    idle: Arc<Mutex<Vec<Worker>>>,
}
struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
}
#[derive(Debug, PartialEq)]
pub enum Failure { Unavailable, Rejected, TooLarge, Timeout, Busy }

// Serialize directly from the immutable snapshot. Each row travels once; Node
// groups references into collection tables instead of receiving duplicated data.
struct Row<'a>(&'a Artifact);
impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let a = self.0;
        let mut row = serializer.serialize_map(None)?;
        for (key, value) in &a.data {
            if !["id", "type", "collection", "revision", "created_at", "updated_at"].contains(&key.as_str()) {
                row.serialize_entry(key, value)?;
            }
        }
        row.serialize_entry("id", &a.id)?;
        row.serialize_entry("type", &a.kind)?;
        row.serialize_entry("collection", &a.kind)?;
        row.serialize_entry("revision", &a.revision)?;
        row.serialize_entry("created_at", &a.created_at)?;
        row.serialize_entry("updated_at", &a.updated_at)?;
        row.end()
    }
}
#[derive(Serialize)]
struct Input<'a> {
    protocol: u8,
    sql: &'a str,
    parameters: &'a [Value],
    records: Vec<Row<'a>>,
    include_count: bool,
}
struct Buffer(Vec<u8>);
impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_INPUT {
            return Err(io::Error::other("query input limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl AlaSql {
    fn new(node: PathBuf, helper: PathBuf, ready: bool) -> Self {
        Self { node, helper, ready, capacity: Semaphore::new(2), idle: Arc::new(Mutex::new(Vec::new())) }
    }
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self::new(executable.clone(), executable, true)
    }
    pub async fn detect(node: PathBuf, helper: PathBuf) -> Self {
        let mut engine = Self::new(node, helper, false);
        // Keep one process warm. A concurrent query opens the second slot;
        // both are reused thereafter. This bounds idle memory on small DBs.
        if let Ok(Ok(worker)) = tokio::time::timeout(Duration::from_secs(5), engine.spawn()).await {
            engine.idle.lock().await.push(worker);
            engine.ready = true;
        }
        engine
    }

    pub async fn execute(&self, sql: String, parameters: Vec<Value>, artifacts: &[Arc<Artifact>], include_count: bool) -> Result<Value, Failure> {
        if !self.ready { return Err(Failure::Unavailable); }
        let _permit = self.capacity.try_acquire().map_err(|_| Failure::Busy)?;
        if artifacts.len() > 500_000 || sql.len() > 65536 { return Err(Failure::TooLarge); }
        let mut encoded = Buffer(Vec::new());
        serde_json::to_writer(&mut encoded, &Input { protocol: 2, sql: &sql, parameters: &parameters,
            records: artifacts.iter().map(|a| Row(a)).collect(), include_count }).map_err(|_| Failure::TooLarge)?;
        let idle = self.idle.lock().await.pop();
        let mut worker = match idle {
            Some(worker) => worker,
            None => tokio::time::timeout(Duration::from_secs(5), self.spawn()).await.map_err(|_| Failure::Timeout)??,
        };
        let result = tokio::time::timeout(Duration::from_secs(5), worker.request(&encoded.0)).await;
        drop(encoded);
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let _ = worker.child.kill().await;
                self.replace_failed_worker();
                return Err(error);
            }
            Err(_) => {
                let _ = worker.child.kill().await;
                self.replace_failed_worker();
                return Err(Failure::Timeout);
            }
        };
        // Rejected SQL is a healthy protocol response. Transport failures and
        // timeouts discard the process; cancellation also kills it via Drop.
        self.idle.lock().await.push(worker);
        match response["error"].as_str() {
            Some("query_rejected") => Err(Failure::Rejected),
            Some("query_too_large") => Err(Failure::TooLarge),
            Some(_) => Err(Failure::Unavailable),
            None => Ok(response),
        }
    }

    fn replace_failed_worker(&self) {
        let idle = self.idle.clone();
        let node = self.node.clone();
        let helper = self.helper.clone();
        tokio::spawn(async move {
            if let Ok(Ok(worker)) = tokio::time::timeout(Duration::from_secs(5), Self::spawn_process(&node, &helper)).await {
                let mut available = idle.lock().await;
                if available.len() < 2 { available.push(worker); }
            }
        });
    }

    async fn spawn(&self) -> Result<Worker, Failure> {
        Self::spawn_process(&self.node, &self.helper).await
    }

    async fn spawn_process(node: &PathBuf, helper: &PathBuf) -> Result<Worker, Failure> {
        let mut command = Command::new(node);
        command.arg("--max-old-space-size=64").arg(helper).arg("--worker").env_clear();
        if let Some(path) = std::env::var_os("PATH") { command.env("PATH", path); }
        #[cfg(windows)]
        if let Some(root) = std::env::var_os("SystemRoot") { command.env("SystemRoot", root); }
        let mut child = command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
            .kill_on_drop(true).spawn().map_err(|_| Failure::Unavailable)?;
        let stdin = child.stdin.take().ok_or(Failure::Unavailable)?;
        let stdout = child.stdout.take().ok_or(Failure::Unavailable)?;
        let mut worker = Worker { child, stdin, stdout };
        let hello = worker.read().await?;
        if hello["status"] != "ready" || hello["protocol"] != 2 { return Err(Failure::Unavailable); }
        Ok(worker)
    }
}
impl Worker {
    async fn request(&mut self, input: &[u8]) -> Result<Value, Failure> {
        self.stdin.write_u32(input.len() as u32).await.map_err(|_| Failure::Unavailable)?;
        self.stdin.write_all(input).await.map_err(|_| Failure::Unavailable)?;
        self.stdin.flush().await.map_err(|_| Failure::Unavailable)?;
        self.read().await
    }
    async fn read(&mut self) -> Result<Value, Failure> {
        let length = self.stdout.read_u32().await.map_err(|_| Failure::Unavailable)? as usize;
        if length > MAX_OUTPUT { return Err(Failure::TooLarge); }
        let mut output = vec![0; length];
        self.stdout.read_exact(&mut output).await.map_err(|_| Failure::Unavailable)?;
        serde_json::from_slice(&output).map_err(|_| Failure::Unavailable)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;

    fn row(id: &str) -> Arc<Artifact> {
        Arc::new(Artifact { id: id.into(), workspace_id: "default".into(), kind: "customers".into(),
            data: json!({"name":"Ada","id":"spoof","collection":"spoof"}).as_object().unwrap().clone(),
            revision: 1, created_at: "2026-01-01".into(), updated_at: "2026-01-01".into() })
    }
    #[tokio::test]
    async fn workers_reuse_processes_and_do_not_retain_other_callers_rows() {
        let engine = AlaSql::detect("node".into(), PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/runtime/alasql.cjs"))).await;
        assert!(engine.ready);
        let before: Vec<_> = engine.idle.lock().await.iter().map(|w| w.child.id()).collect();
        assert_eq!(before.len(), 1);
        for _ in 0..5 {
            let result = engine.execute("SELECT id, collection, name FROM customers".into(), vec![], &[row("actual")], false).await.unwrap();
            assert_eq!(result["items"], json!([{"id":"actual","collection":"customers","name":"Ada"}]));
            let result = engine.execute("SELECT COUNT(*) AS total FROM records".into(), vec![], &[], false).await.unwrap();
            assert_eq!(result["items"], json!([{"total":0}]));
        }
        assert_eq!(engine.execute("SELECT * FROM secrets".into(), vec![], &[row("a")], false).await, Err(Failure::Rejected));
        let after: Vec<_> = engine.idle.lock().await.iter().map(|w| w.child.id()).collect();
        assert_eq!(before, after);
        // A terminated worker is discarded, and following requests remain usable.
        engine.idle.lock().await.last_mut().unwrap().child.kill().await.unwrap();
        assert_eq!(engine.execute("SELECT id FROM records".into(), vec![], &[row("a")], false).await, Err(Failure::Unavailable));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if engine.idle.lock().await.len() == 1 { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        assert!(engine.execute("SELECT id FROM records".into(), vec![], &[row("a")], false).await.is_ok());
    }
    #[tokio::test]
    async fn timeout_and_cancellation_discard_the_worker_and_release_capacity() {
        use std::os::unix::fs::PermissionsExt;
        let directory = Directory::new();
        let path = directory.0.join("worker");
        std::fs::write(&path, r#"#!/usr/bin/env python3
import json, sys, time
output = sys.stdout.buffer
input = sys.stdin.buffer
def send(value):
    data = json.dumps(value).encode()
    output.write(len(data).to_bytes(4,'big') + data); output.flush()
def read(size):
    data = b''
    while len(data) < size:
        part = input.read(size - len(data))
        if not part: raise EOFError()
        data += part
    return data
send({'status':'ready','protocol':2})
while True:
    try: data = json.loads(read(int.from_bytes(read(4),'big')))
    except EOFError: break
    if data['sql'] == 'hang': time.sleep(30)
    send({'items':data['records'], 'total':None})
"#).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let engine = Arc::new(AlaSql::fixture(path));
        assert_eq!(engine.execute("hang".into(), vec![], &[], false).await, Err(Failure::Timeout));
        assert!(engine.execute("ok".into(), vec![], &[], false).await.is_ok());
        let task_engine = engine.clone();
        let task = tokio::spawn(async move { task_engine.execute("hang".into(), vec![], &[], false).await });
        let task_engine = engine.clone();
        let second = tokio::spawn(async move { task_engine.execute("hang".into(), vec![], &[], false).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(engine.capacity.available_permits(), 0);
        assert_eq!(engine.execute("ok".into(), vec![], &[], false).await, Err(Failure::Busy));
        task.abort(); second.abort();
        let _ = task.await; let _ = second.await;
        assert_eq!(engine.capacity.available_permits(), 2);
        assert!(engine.execute("ok".into(), vec![], &[], false).await.is_ok());
    }
}
