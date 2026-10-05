use super::{App, RequestId, accounts, events};
use crate::storage::{EventWorker, Store, StoredFile, hex, new_id, private_file};
use chrono::Utc;
use rand::{RngCore, rngs::OsRng};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, io::{self, Write}, path::Path};

fn save_json(path: &Path, value: &Value) -> io::Result<()> {
    let mut file = private_file(path, false)?;
    file.set_len(0)?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(io::Error::other)?)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

/// Seed a new database once, or resume an interrupted bootstrap. Force permits
/// adding the same starter set to an existing database without replacing data.
pub fn seed_default_demo(
    store: &mut Store,
    directory: &Path,
    workspace: &str,
    owner: &str,
    base_url: &str,
    force: bool,
) -> io::Result<Option<Value>> {
    if store.demo_event().is_some() {
        return Ok(None);
    }
    let marker = directory.join("demo-seed.json");
    if !force && !store.is_empty()? && !marker.exists() {
        return Ok(None);
    }
    if !marker.exists() {
        save_json(&marker, &json!({"version":1,"complete":false}))?;
    }
    let record = store.create(
        workspace,
        "demo".into(),
        json!({"title":"Welcome to NioDB","message":"Open /guide to try records, users, files and TypeScript workers.","status":"ready","is_public":true})
            .as_object().unwrap().clone(),
        Some("niodb-starter-record-v1".into()),
    )?;

    let credentials = directory.join("demo-user.json");
    let user = if let Some(user) = store.find_user(workspace, "niodb_demo") {
        user
    } else {
        let login = if credentials.exists() {
            serde_json::from_slice::<Value>(&fs::read(&credentials)?).map_err(io::Error::other)?
        } else {
            let mut random = [0u8; 24];
            OsRng.fill_bytes(&mut random);
            let login = json!({"username":"niodb_demo","password":hex(&random)});
            save_json(&credentials, &login)?;
            login
        };
        let password = login["password"].as_str().filter(|password| password.len() >= 12)
            .ok_or_else(|| io::Error::other("demo-user.json is missing its generated password"))?;
        store.create_user(workspace.into(), "niodb_demo".into(), accounts::hash_password(password))?
    };

    let content = b"Welcome to NioDB!\n\nThis is your demo file.\nOpen /guide for records, user login, file storage, and TypeScript workers.\nPublish demo.ping to run the example worker's fetch() call to /health.\n";
    let sha256 = hex(&Sha256::digest(content));
    let file = if let Some(file) = store.files(workspace, "demo").into_iter()
        .find(|file| file.filename == "welcome.txt" && file.sha256 == sha256) {
        file
    } else {
        let mut upload = store.begin_upload()?;
        upload.file.write_all(content)?;
        upload.file.sync_all()?;
        store.finish_upload(StoredFile {
            id: new_id("file"), artifact_id: String::new(), workspace_id: workspace.into(),
            bucket: "demo".into(), filename: "welcome.txt".into(), sha256,
            size: content.len() as u64, mime: "text/plain".into(),
            created_at: Utc::now().to_rfc3339(), created_by: owner.into(), user_id: None,
        }, &upload)?
    };
    let worker = if let Some(worker) = store.event_workers().into_iter()
        .find(|worker| worker.source.starts_with("// NioDB starter worker: REST fetch example")) {
        worker
    } else {
        let worker = EventWorker {
            id: new_id("wrk"), event_name: "demo.ping".into(),
            source: include_str!("../../runtime/demo-worker.ts").replace("__NIODB_BASE_URL__", base_url),
            created_at: Utc::now().to_rfc3339(), created_by: owner.into(),
        };
        store.create_event_worker(worker.clone())?;
        worker
    };
    let event = json!({"id":new_id("evt"),"name":"demo.ping", "actor":owner,"created_at":Utc::now().to_rfc3339(),
        "data":{"message":"Welcome! This demo event calls the local REST health API.","record_id":record.id,"source":"niodb-starter"}});
    store.finish_demo_seed(event.clone())?;
    save_json(&marker, &json!({"version":1,"complete":true,"record_id":record.id,"user_id":user.id,
        "file_id":file.id,"worker_id":worker.id,"event_id":event["id"]}))?;
    Ok(Some(event))
}

pub fn dispatch_demo_event(app: App, event: Value) {
    tokio::spawn(async move {
        // Allow the HTTP listener to begin serving before the worker fetches it.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        if events::dispatch_event(&app, &RequestId(new_id("req")), event).await.is_err() {
            eprintln!("demo event delivery failed");
        }
    });
}
