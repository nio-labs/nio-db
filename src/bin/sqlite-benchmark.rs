//! Matched HTTP workload adapter for SQLite. This is benchmark tooling, not a NioDB server.
use axum::{body::Bytes, extract::{Path, Query, State}, http::{HeaderMap, StatusCode}, routing::{get, post}, Json, Router};
use chrono::Utc;
use rand::random;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{collections::HashMap, env, path::PathBuf, sync::{Arc, Mutex}};

type App = Arc<Adapter>;
type ApiResult = Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)>;
struct Adapter { db: Mutex<Connection>, token: String }
fn error(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) { (status, Json(json!({"error":message}))) }
fn authorized(headers: &HeaderMap, app: &App) -> Result<(), (StatusCode, Json<Value>)> {
    if headers.get("authorization").and_then(|v| v.to_str().ok()) == Some(&format!("Bearer {}", app.token)) { Ok(()) }
    else { Err(error(StatusCode::UNAUTHORIZED, "Authentication required")) }
}
fn db_error(_: rusqlite::Error) -> (StatusCode, Json<Value>) { error(StatusCode::INTERNAL_SERVER_ERROR, "SQLite operation failed") }
fn id(prefix: &str) -> String { format!("{}_{:032x}", prefix, random::<u128>()) }
fn now() -> String { Utc::now().to_rfc3339() }
fn record(id: &str, collection: &str, data: Value, created_at: &str) -> Value {
    json!({"id":id,"collection":collection,"data":data,"created_at":created_at,"updated_at":created_at,"revision":1})
}
fn insert_record(conn: &Connection, collection: &str, data: &Value) -> Result<Value, rusqlite::Error> {
    let rec_id = id("art"); let created = now();
    conn.execute("INSERT INTO records (id,collection,data,created_at) VALUES (?1,?2,?3,?4)", params![rec_id,collection,data.to_string(),created])?;
    if collection == "customers" {
        conn.execute("INSERT INTO customers (id,plan,credits,name,email) VALUES (?1,?2,?3,?4,?5)", params![rec_id,data["plan"].as_str(),data["credits"].as_i64(),data["name"].as_str(),data["email"].as_str()])?;
    } else if collection == "telemetry" {
        conn.execute("INSERT INTO telemetry (id,service,status,latency_ms) VALUES (?1,?2,?3,?4)", params![rec_id,data["service"].as_str(),data["status"].as_str(),data["latency_ms"].as_i64()])?;
    }
    Ok(record(&rec_id, collection, data.clone(), &created))
}
async fn health(State(app): State<App>) -> ApiResult {
    let db = app.db.lock().unwrap();
    let version: String = db.query_row("SELECT sqlite_version()", [], |r| r.get(0)).map_err(db_error)?;
    Ok((StatusCode::OK, Json(json!({"status":"ok","engine":"SQLite","sqlite_version":version,"journal_mode":"wal","synchronous":"full"}))))
}
async fn create_bucket(State(app):State<App>, headers:HeaderMap, Json(body):Json<Value>) -> ApiResult {
    authorized(&headers,&app)?;
    Ok((StatusCode::CREATED, Json(json!({"name":body["name"]}))))
}
async fn upload(State(app):State<App>, Path(bucket):Path<String>, Query(query):Query<HashMap<String,String>>, headers:HeaderMap, body:Bytes) -> ApiResult {
    authorized(&headers,&app)?;
    let filename=query.get("filename").cloned().unwrap_or_default();
    let mime=headers.get("content-type").and_then(|v|v.to_str().ok()).unwrap_or("application/octet-stream").to_string();
    let file_id=id("file"); let size=body.len();
    let mut db=app.db.lock().unwrap();
    let tx=db.transaction().map_err(db_error)?;
    tx.execute("INSERT INTO files (id,bucket,filename,mime,bytes) VALUES (?1,?2,?3,?4,?5)",params![file_id,bucket,filename,mime,body.as_ref()]).map_err(db_error)?;
    let data=json!({"file_id":file_id,"bucket":bucket,"filename":filename,"size":size,"content_type":mime});
    insert_record(&tx,"files",&data).map_err(db_error)?;
    tx.commit().map_err(db_error)?;
    Ok((StatusCode::CREATED,Json(json!({"id":file_id,"size":size,"filename":filename,"mime":mime}))))
}
async fn download(State(app):State<App>,Path((bucket,file_id)):Path<(String,String)>,headers:HeaderMap)->Result<(StatusCode,Bytes),(StatusCode,Json<Value>)>{
    authorized(&headers,&app)?;
    let db=app.db.lock().unwrap();
    let bytes:Vec<u8>=db.query_row("SELECT bytes FROM files WHERE bucket=?1 AND id=?2",params![bucket,file_id],|r|r.get(0)).map_err(db_error)?;
    Ok((StatusCode::OK,Bytes::from(bytes)))
}
async fn bulk(State(app):State<App>,headers:HeaderMap,Json(items):Json<Vec<Value>>)->ApiResult{
    authorized(&headers,&app)?;
    let mut db=app.db.lock().unwrap();
    let tx=db.transaction().map_err(db_error)?;
    let mut records=Vec::with_capacity(items.len());
    for item in &items {
        let collection=item["collection"].as_str().ok_or_else(||error(StatusCode::BAD_REQUEST,"collection required"))?;
        let data=item.get("data").ok_or_else(||error(StatusCode::BAD_REQUEST,"data required"))?;
        records.push(insert_record(&tx,collection,data).map_err(db_error)?);
    }
    tx.commit().map_err(db_error)?;
    Ok((StatusCode::OK,Json(json!({"inserted":records.len(),"records":records}))))
}
async fn list(State(app):State<App>,headers:HeaderMap,Query(query):Query<HashMap<String,String>>)->ApiResult{
    authorized(&headers,&app)?;
    let collection=query.get("collection");
    let limit=query.get("limit").and_then(|x|x.parse::<i64>().ok()).unwrap_or(25);
    let db=app.db.lock().unwrap();
    let total:i64=if let Some(c)=collection { db.query_row("SELECT COUNT(*) FROM records WHERE collection=?1",params![c],|r|r.get(0)).map_err(db_error)? }
        else {db.query_row("SELECT COUNT(*) FROM records",[],|r|r.get(0)).map_err(db_error)?};
    let mut items=Vec::new();
    let sql=if collection.is_some(){"SELECT id,collection,data,created_at FROM records WHERE collection=?1 ORDER BY created_at,id LIMIT ?2"}
        else {"SELECT id,collection,data,created_at FROM records ORDER BY created_at,id LIMIT ?2"};
    let mut stmt=db.prepare(sql).map_err(db_error)?;
    let mut rows=if let Some(c)=collection{stmt.query(params![c,limit]).map_err(db_error)?}else{stmt.query(params![rusqlite::types::Null,limit]).map_err(db_error)?};
    while let Some(row)=rows.next().map_err(db_error)? {items.push(row_record(row).map_err(db_error)?);}
    Ok((StatusCode::OK,Json(json!({"items":items,"total":total,"limit":limit,"page":1}))))
}
fn row_record(row:&rusqlite::Row<'_>)->Result<Value,rusqlite::Error>{
    let id:String=row.get(0)?;let collection:String=row.get(1)?;let data:String=row.get(2)?;let created:String=row.get(3)?;
    Ok(record(&id,&collection,serde_json::from_str(&data).unwrap_or(Value::Null),&created))
}
async fn get_record(State(app):State<App>,Path(rec_id):Path<String>,headers:HeaderMap)->ApiResult{
    authorized(&headers,&app)?;
    let db=app.db.lock().unwrap();
    let value=db.query_row("SELECT id,collection,data,created_at FROM records WHERE id=?1",params![rec_id],row_record).map_err(db_error)?;
    Ok((StatusCode::OK,Json(value)))
}
async fn search(State(app):State<App>,headers:HeaderMap,Json(body):Json<Value>)->ApiResult{
    authorized(&headers,&app)?;
    let vector:Vec<f64>=body["vector"].as_array().ok_or_else(||error(StatusCode::BAD_REQUEST,"vector required"))?.iter().filter_map(Value::as_f64).collect();
    let min_score=body["min_score"].as_f64().unwrap_or(-1.0);
    let top_k=body["top_k"].as_u64().unwrap_or(10) as usize;
    let db=app.db.lock().unwrap();
    let mut stmt=db.prepare("SELECT id,collection,data,created_at FROM records WHERE collection='ai_memories'").map_err(db_error)?;
    let mut rows=stmt.query([]).map_err(db_error)?;
    let mut ranked=Vec::new();
    while let Some(row)=rows.next().map_err(db_error)?{
        let mut rec=row_record(row).map_err(db_error)?;
        let embedding:Vec<f64>=rec["data"]["embedding"].as_array().map(|a|a.iter().filter_map(Value::as_f64).collect()).unwrap_or_default();
        if embedding.len()!=vector.len(){continue;}
        let dot:f64=vector.iter().zip(&embedding).map(|(a,b)|a*b).sum();
        let norm=f64::sqrt(vector.iter().map(|x|x*x).sum::<f64>()*embedding.iter().map(|x|x*x).sum::<f64>());
        let score=if norm>0.0{dot/norm}else{0.0};
        if score>=min_score{rec["score"]=json!(score);ranked.push((score,rec));}
    }
    ranked.sort_by(|a,b|b.0.total_cmp(&a.0));
    let items:Vec<Value>=ranked.into_iter().take(top_k).map(|(_,v)|v).collect();
    Ok((StatusCode::OK,Json(json!({"items":items}))))
}
async fn sql(State(app):State<App>,headers:HeaderMap,Json(body):Json<Value>)->ApiResult{
    authorized(&headers,&app)?;
    let query=body["sql"].as_str().ok_or_else(||error(StatusCode::BAD_REQUEST,"sql required"))?;
    const QUERIES:[&str;3]=[
        "SELECT plan, COUNT(*) AS user_count, AVG(credits) AS avg_credits FROM customers GROUP BY plan",
        "SELECT service, AVG(latency_ms) AS avg_latency FROM telemetry WHERE status = 'ok' GROUP BY service",
        "SELECT name, email, credits FROM customers ORDER BY credits DESC LIMIT 5"];
    if !QUERIES.contains(&query){return Err(error(StatusCode::BAD_REQUEST,"unsupported benchmark query"));}
    let db=app.db.lock().unwrap();
    let mut stmt=db.prepare(query).map_err(db_error)?;
    let columns:Vec<String>=stmt.column_names().iter().map(|s|s.to_string()).collect();
    let rows=stmt.query_map([],|row|{
        let mut value=serde_json::Map::new();
        for (i,key) in columns.iter().enumerate(){
            let cell=row.get_ref(i)?;
            let v=match cell{
                rusqlite::types::ValueRef::Null=>Value::Null,
                rusqlite::types::ValueRef::Integer(x)=>json!(x),
                rusqlite::types::ValueRef::Real(x)=>json!(x),
                rusqlite::types::ValueRef::Text(x)=>json!(String::from_utf8_lossy(x).to_string()),
                rusqlite::types::ValueRef::Blob(_)=>Value::Null};
            value.insert(key.clone(),v);
        }
        Ok(Value::Object(value))
    }).map_err(db_error)?;
    let items:Result<Vec<_>,_>=rows.collect();
    Ok((StatusCode::OK,Json(json!({"items":items.map_err(db_error)?}))))
}
async fn event(State(app):State<App>,headers:HeaderMap,Json(body):Json<Value>)->ApiResult{
    authorized(&headers,&app)?;
    Ok((StatusCode::ACCEPTED,Json(json!({"id":id("evt"),"name":body["name"],"data":body["data"]}))))
}
#[tokio::main]
async fn main(){
    let mut dir=None;let mut listen=None;
    let mut args=env::args().skip(1);
    while let Some(a)=args.next(){match a.as_str(){"--dir"=>dir=args.next(),"--listen"=>listen=args.next(),_=>panic!("Unknown argument: {a}")}}
    let dir=PathBuf::from(dir.expect("--dir is required"));std::fs::create_dir_all(&dir).unwrap();
    let db=Connection::open(dir.join("comparison.sqlite")).unwrap();
    let mode:String=db.query_row("PRAGMA journal_mode=WAL",[],|r|r.get(0)).unwrap();assert_eq!(mode,"wal");
    db.execute_batch("PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY,collection TEXT NOT NULL,data TEXT NOT NULL,created_at TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS records_collection ON records(collection);
      CREATE TABLE IF NOT EXISTS telemetry(id TEXT PRIMARY KEY,service TEXT,status TEXT,latency_ms INTEGER);
      CREATE TABLE IF NOT EXISTS customers(id TEXT PRIMARY KEY,plan TEXT,credits INTEGER,name TEXT,email TEXT);
      CREATE TABLE IF NOT EXISTS files(id TEXT PRIMARY KEY,bucket TEXT,filename TEXT,mime TEXT,bytes BLOB);").unwrap();
    let app=Arc::new(Adapter{db:Mutex::new(db),token:env::var("NIODB_BENCHMARK_TOKEN").expect("NIODB_BENCHMARK_TOKEN required")});
    let router=Router::new().route("/health",get(health)).route("/api/v1/records",get(list))
      .route("/api/v1/records/bulk",post(bulk)).route("/api/v1/records/search",post(search))
      .route("/api/v1/records/:id",get(get_record)).route("/api/v1/storage",post(create_bucket))
      .route("/api/v1/storage/:bucket/upload",post(upload)).route("/api/v1/storage/:bucket/:id",get(download))
      .route("/api/v1/query",post(sql)).route("/api/v1/events",post(event)).with_state(app);
    let listener=tokio::net::TcpListener::bind(listen.expect("--listen required")).await.unwrap();
    axum::serve(listener,router).await.unwrap();
}
