use crate::storage::Artifact;
use serde_json::{Map, Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
};

pub struct AlaSql {
    node: PathBuf,
    helper: PathBuf,
    pub ready: bool,
    capacity: Semaphore,
}
#[derive(Debug, PartialEq)]
pub enum Failure {
    Unavailable,
    Rejected,
    TooLarge,
    Timeout,
    Busy,
}

impl AlaSql {
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self {
            node: executable.clone(),
            helper: executable,
            ready: true,
            capacity: Semaphore::new(2),
        }
    }
    pub async fn detect(node: PathBuf, helper: PathBuf) -> Self {
        let mut engine = Self {
            node,
            helper,
            ready: false,
            capacity: Semaphore::new(2),
        };
        engine.ready = engine
            .call(None)
            .await
            .is_ok_and(|value| value["status"] == "ready");
        engine
    }

    pub async fn execute(
        &self,
        sql: String,
        parameters: Vec<Value>,
        artifacts: &[Artifact],
        include_count: bool,
    ) -> Result<Value, Failure> {
        if !self.ready {
            return Err(Failure::Unavailable);
        }
        let _permit = self.capacity.try_acquire().map_err(|_| Failure::Busy)?;
        if artifacts.len() > 500_000 || sql.len() > 65536 {
            return Err(Failure::TooLarge);
        }
        let mut tables = Map::<String, Value>::new();
        let mut all = Vec::new();
        for artifact in artifacts {
            let mut row = artifact.data.clone();
            row.insert("id".into(), json!(artifact.id));
            row.insert("type".into(), json!(artifact.kind));
            row.insert("revision".into(), json!(artifact.revision));
            row.insert("created_at".into(), json!(artifact.created_at));
            row.insert("updated_at".into(), json!(artifact.updated_at));
            if !["__proto__", "constructor", "prototype"].contains(&artifact.kind.as_str())
                && artifact.kind != "artifacts"
            {
                tables
                    .entry(artifact.kind.clone())
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .unwrap()
                    .push(json!(row));
            }
            all.push(json!(row));
        }
        tables.insert("artifacts".into(), json!(all));
        let input = json!({"protocol":1,"sql":sql,"parameters":parameters,"tables":tables,"include_count":include_count});
        let encoded = serde_json::to_vec(&input).map_err(|_| Failure::Rejected)?;
        if encoded.len() > 64 * 1024 * 1024 {
            return Err(Failure::TooLarge);
        }
        self.call(Some(encoded)).await
    }

    async fn call(&self, input: Option<Vec<u8>>) -> Result<Value, Failure> {
        let mut command = Command::new(&self.node);
        command
            .arg("--max-old-space-size=64")
            .arg(&self.helper)
            .env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        #[cfg(windows)]
        if let Some(root) = std::env::var_os("SystemRoot") {
            command.env("SystemRoot", root);
        }
        if input.is_none() {
            command.arg("--check");
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| Failure::Unavailable)?;
        let mut stdin = child.stdin.take().ok_or(Failure::Unavailable)?;
        let stdout = child.stdout.take().ok_or(Failure::Unavailable)?;
        let stderr = child.stderr.take().ok_or(Failure::Unavailable)?;
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::try_join!(
                async {
                    if let Some(input) = input {
                        stdin
                            .write_all(&input)
                            .await
                            .map_err(|_| Failure::Unavailable)?;
                    }
                    drop(stdin);
                    Ok::<_, Failure>(())
                },
                bounded(stdout),
                bounded(stderr),
                async { child.wait().await.map_err(|_| Failure::Unavailable) }
            )
        })
        .await;
        let (_, output, _, status) = match result {
            Err(_) => {
                let _ = child.kill().await;
                return Err(Failure::Timeout);
            }
            Ok(Err(error)) => {
                let _ = child.kill().await;
                return Err(error);
            }
            Ok(Ok(result)) => result,
        };
        let value: Value = serde_json::from_slice(&output).map_err(|_| Failure::Unavailable)?;
        if !status.success() {
            return Err(match value["error"].as_str() {
                Some("query_rejected") => Failure::Rejected,
                Some("query_too_large") => Failure::TooLarge,
                _ => Failure::Unavailable,
            });
        }
        Ok(value)
    }
}

async fn bounded(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>, Failure> {
    let mut output = Vec::new();
    reader
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut output)
        .await
        .map_err(|_| Failure::Unavailable)?;
    if output.len() > 4 * 1024 * 1024 {
        return Err(Failure::TooLarge);
    }
    Ok(output)
}
