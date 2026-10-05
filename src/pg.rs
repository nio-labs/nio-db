use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use serde_json::Value;
use tokio::net::TcpListener;

use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, StartupHandler,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireConnectionState, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::{Authentication, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

use crate::api::App;
use crate::auth::Principal;
use crate::query::AlaSql;
use crate::storage::Store;

pub struct NioPgHandler {
    store: Arc<Mutex<Store>>,
    query: Arc<AlaSql>,
    workspace_id: String,
    query_parser: Arc<NoopQueryParser>,
}

impl NioPgHandler {
    pub fn new(store: Arc<Mutex<Store>>, query: Arc<AlaSql>, workspace_id: String) -> Self {
        Self {
            store,
            query,
            workspace_id,
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    pub async fn execute_query(&self, raw_query: &str) -> PgWireResult<Response> {
        let trimmed = raw_query.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return Ok(Response::EmptyQuery);
        }

        let upper = trimmed.to_ascii_uppercase();

        // 1. Transaction and Session Commands
        if upper == "BEGIN" || upper.starts_with("BEGIN ") || upper.starts_with("START TRANSACTION") {
            return Ok(Response::Execution(Tag::new("BEGIN")));
        }
        if upper == "COMMIT" || upper.starts_with("COMMIT ") {
            return Ok(Response::Execution(Tag::new("COMMIT")));
        }
        if upper == "ROLLBACK" || upper.starts_with("ROLLBACK ") {
            return Ok(Response::Execution(Tag::new("ROLLBACK")));
        }
        if upper.starts_with("SET ") {
            return Ok(Response::Execution(Tag::new("SET")));
        }
        if upper.starts_with("DISCARD ") || upper.starts_with("RESET ") {
            return Ok(Response::Execution(Tag::new("RESET")));
        }

        // 2. SHOW Commands
        if upper.starts_with("SHOW ") {
            let var = trimmed[4..].trim().trim_matches('\'').trim();
            let var_upper = var.to_ascii_uppercase();
            let value = match var_upper.as_str() {
                "SEARCH_PATH" => "public",
                "TRANSACTION ISOLATION LEVEL" | "TRANSACTION_ISOLATION" | "DEFAULT_TRANSACTION_ISOLATION" => "read committed",
                "SERVER_VERSION" => "15.0 (NioDB)",
                "SERVER_ENCODING" | "CLIENT_ENCODING" => "UTF8",
                "STANDARD_CONFORMING_STRINGS" => "on",
                "INTEGER_DATETIMES" => "on",
                "MAX_IDENTIFIER_LENGTH" => "63",
                _ => "",
            };

            let schema = Arc::new(vec![FieldInfo::new(
                var.to_lowercase(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&value)?;
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        // 3. Built-in Functions & Session Helpers
        if upper == "SELECT 1" || upper.starts_with("SELECT 1 ") || upper.starts_with("SELECT 1 AS") {
            let schema = Arc::new(vec![FieldInfo::new(
                "?column?".into(),
                None,
                None,
                Type::INT4,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&1i32)?;
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        if upper.contains("VERSION()") {
            let schema = Arc::new(vec![FieldInfo::new(
                "version".into(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            let v = "PostgreSQL 15.0 (NioDB v1.0.2)";
            encoder.encode_field(&v)?;
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        if upper.contains("CURRENT_DATABASE()") {
            let schema = Arc::new(vec![FieldInfo::new(
                "current_database".into(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            let db = "niodb";
            encoder.encode_field(&db)?;
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        if upper.contains("CURRENT_SCHEMA()") {
            let schema = Arc::new(vec![FieldInfo::new(
                "current_schema".into(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            let sch = "public";
            encoder.encode_field(&sch)?;
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        // 4. PostgreSQL Catalog Queries (DBeaver / TablePlus / DB Explorer support)
        if upper.contains("PG_NAMESPACE") || upper.contains("INFORMATION_SCHEMA.SCHEMATA") {
            let schema = Arc::new(vec![
                FieldInfo::new("oid".into(), None, None, Type::INT4, FieldFormat::Text),
                FieldInfo::new("nspname".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("nspowner".into(), None, None, Type::INT4, FieldFormat::Text),
            ]);
            let schemas = [
                (2200i32, "public", 10i32),
                (11i32, "pg_catalog", 10i32),
                (12i32, "information_schema", 10i32),
            ];
            let mut rows = Vec::new();
            for (oid, name, owner) in schemas {
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field(&oid)?;
                encoder.encode_field(&name)?;
                encoder.encode_field(&owner)?;
                rows.push(Ok(encoder.take_row()));
            }
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        if upper.contains("PG_DATABASE") {
            let schema = Arc::new(vec![
                FieldInfo::new("datname".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("datdba".into(), None, None, Type::INT4, FieldFormat::Text),
                FieldInfo::new("encoding".into(), None, None, Type::INT4, FieldFormat::Text),
            ]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&"niodb")?;
            encoder.encode_field(&10i32)?;
            encoder.encode_field(&6i32)?; // UTF8
            let rows = vec![Ok(encoder.take_row())];
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        if upper.contains("PG_TABLES") || upper.contains("INFORMATION_SCHEMA.TABLES") {
            let collections = {
                let store = self.store.lock().unwrap();
                let mut cols = store.collections(&self.workspace_id);
                if !cols.iter().any(|c| c == "records") {
                    cols.push("records".into());
                }
                cols
            };

            let schema = Arc::new(vec![
                FieldInfo::new("schemaname".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("tablename".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("tableowner".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("tablespace".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                FieldInfo::new("hasindexes".into(), None, None, Type::BOOL, FieldFormat::Text),
                FieldInfo::new("hasrules".into(), None, None, Type::BOOL, FieldFormat::Text),
                FieldInfo::new("hastriggers".into(), None, None, Type::BOOL, FieldFormat::Text),
                FieldInfo::new("rowsecurity".into(), None, None, Type::BOOL, FieldFormat::Text),
            ]);

            let mut rows = Vec::new();
            for col in collections {
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field(&"public")?;
                encoder.encode_field(&col.as_str())?;
                encoder.encode_field(&"niodb")?;
                encoder.encode_field(&None::<String>)?;
                encoder.encode_field(&false)?;
                encoder.encode_field(&false)?;
                encoder.encode_field(&false)?;
                encoder.encode_field(&false)?;
                rows.push(Ok(encoder.take_row()));
            }
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
        }

        // 5. Data Queries (Native Rust AST Engine with AlaSQL Fallback)
        let table_hint = crate::query::extract_table_from_sql(trimmed);
        let collection_filter = match table_hint.as_deref() {
            Some(t) if !t.eq_ignore_ascii_case("records") && !t.eq_ignore_ascii_case("artifacts") => {
                Some(t.to_string())
            }
            _ => None,
        };

        let (native_res, fallback_records) = {
            let store = self.store.lock().unwrap();
            let refs = store.visible_refs(&self.workspace_id, collection_filter.as_deref(), None);
            match crate::query::execute_native_sql_refs(trimmed, &[], &refs, false) {
                Ok(result) => (Ok(result), None),
                Err(crate::query::NativeSqlError::QueryRejected) => (Err(true), None),
                Err(crate::query::NativeSqlError::Unsupported) => {
                    (Err(false), Some(store.snapshot_visible(&self.workspace_id, None, None)))
                }
            }
        };

        let query_result: Value = match native_res {
            Ok(res) => res,
            Err(true) => {
                return Err(PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax error or query rejected".to_owned(),
                ))));
            }
            Err(false) => {
                let records = fallback_records.unwrap_or_default();
                self.query
                    .execute(trimmed.to_string(), vec![], &records, false)
                    .await
                    .map_err(|e| {
                        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42000".to_owned(),
                            format!("SQL execution error: {e:?}"),
                        )))
                    })?
            }
        };

        // Format Result Rows into Postgres Wire DataRows
        let items = query_result
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if items.is_empty() {
            let schema = Arc::new(vec![FieldInfo::new(
                "result".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            )]);
            return Ok(Response::Query(QueryResponse::new(schema, stream::iter(vec![]))));
        }

        // Infer Columns and Types from Result Items
        let first_row = match items[0].as_object() {
            Some(obj) => obj,
            None => {
                let schema = Arc::new(vec![FieldInfo::new(
                    "value".into(),
                    None,
                    None,
                    Type::VARCHAR,
                    FieldFormat::Text,
                )]);
                let mut rows = Vec::new();
                for item in &items {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    let s = item.to_string();
                    encoder.encode_field(&s.as_str())?;
                    rows.push(Ok(encoder.take_row()));
                }
                return Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))));
            }
        };

        let column_names: Vec<String> = crate::query::project_column_names(trimmed)
            .unwrap_or_else(|| first_row.keys().cloned().collect());
        let mut field_infos = Vec::new();

        for col in &column_names {
            // Sample type from the first non-null occurrence
            let mut col_type = Type::VARCHAR;
            for item in items.iter().take(10) {
                if let Some(val) = item.get(col) {
                    match val {
                        Value::Bool(_) => {
                            col_type = Type::BOOL;
                            break;
                        }
                        Value::Number(n) if n.is_i64() => {
                            col_type = Type::INT8;
                            break;
                        }
                        Value::Number(_) => {
                            col_type = Type::FLOAT8;
                            break;
                        }
                        Value::Array(_) | Value::Object(_) => {
                            col_type = Type::JSON;
                            break;
                        }
                        Value::String(_) => {
                            col_type = Type::VARCHAR;
                            break;
                        }
                        Value::Null => {}
                    }
                }
            }

            field_infos.push(FieldInfo::new(
                col.clone(),
                None,
                None,
                col_type,
                FieldFormat::Text,
            ));
        }

        let schema = Arc::new(field_infos);
        let mut rows = Vec::with_capacity(items.len());

        for item in &items {
            let row_obj = match item.as_object() {
                Some(obj) => obj,
                None => continue,
            };

            let mut encoder = DataRowEncoder::new(schema.clone());
            for (idx, col) in column_names.iter().enumerate() {
                let val = row_obj.get(col);
                let col_type = schema[idx].datatype();

                match val {
                    None | Some(Value::Null) => {
                        encoder.encode_field(&None::<i8>)?;
                    }
                    Some(Value::Bool(b)) => {
                        encoder.encode_field(b)?;
                    }
                    Some(Value::Number(n)) => {
                        if col_type == &Type::INT8 {
                            let i = n.as_i64().unwrap_or(0);
                            encoder.encode_field(&i)?;
                        } else if col_type == &Type::FLOAT8 {
                            let f = n.as_f64().unwrap_or(0.0);
                            encoder.encode_field(&f)?;
                        } else {
                            let s = n.to_string();
                            encoder.encode_field(&s.as_str())?;
                        }
                    }
                    Some(Value::String(s)) => {
                        encoder.encode_field(&s.as_str())?;
                    }
                    Some(v) => {
                        let s = v.to_string();
                        encoder.encode_field(&s.as_str())?;
                    }
                }
            }
            rows.push(Ok(encoder.take_row()));
        }

        Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))))
    }
}

pub struct NioPgStartupHandler {
    principals: Arc<Vec<Principal>>,
    pg_password: Option<String>,
    parameter_provider: DefaultServerParameterProvider,
}

impl NioPgStartupHandler {
    pub fn new(principals: Arc<Vec<Principal>>, pg_password: Option<String>) -> Self {
        Self {
            principals,
            pg_password,
            parameter_provider: DefaultServerParameterProvider::default(),
        }
    }

    pub fn verify_password(&self, input: &str) -> bool {
        if input.is_empty() {
            return false;
        }
        if let Some(ref expected) = self.pg_password {
            if input == expected {
                return true;
            }
        }
        crate::auth::authenticate(&self.principals, input).is_some()
    }
}

#[async_trait]
impl StartupHandler for NioPgStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                if self.verify_password(&pwd.password) {
                    client.set_pid_and_secret_key(1001, SecretKey::I32(2002));
                    finish_authentication(client, &self.parameter_provider).await?;
                } else {
                    let user = client.metadata().get("user").cloned().unwrap_or_default();
                    return Err(PgWireError::InvalidPassword(user));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for NioPgHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let resp = self.execute_query(query).await?;
        Ok(vec![resp])
    }
}

#[async_trait]
impl ExtendedQueryHandler for NioPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement;
        self.execute_query(query).await
    }
}

pub struct NioPgHandlerFactory {
    handler: Arc<NioPgHandler>,
    startup: Arc<NioPgStartupHandler>,
}

impl NioPgHandlerFactory {
    pub fn new(handler: Arc<NioPgHandler>, startup: Arc<NioPgStartupHandler>) -> Self {
        Self { handler, startup }
    }
}

impl PgWireServerHandlers for NioPgHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }
}

pub async fn run_pg_server(
    app: App,
    listen_addr: std::net::SocketAddr,
    pg_password: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workspace_id = crate::api::server_workspace(&app);
    let handler = Arc::new(NioPgHandler::new(
        app.store.clone(),
        app.query.clone(),
        workspace_id,
    ));
    let startup = Arc::new(NioPgStartupHandler::new(
        app.principals.clone(),
        pg_password,
    ));
    let factory = Arc::new(NioPgHandlerFactory::new(handler, startup));

    let listener = TcpListener::bind(listen_addr).await?;
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("pgwire accept error: {e}");
                continue;
            }
        };

        let factory_ref = factory.clone();
        tokio::spawn(async move {
            let _ = process_socket(socket, None, factory_ref).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Directory;
    use serde_json::json;

    fn test_handler() -> (Directory, NioPgHandler) {
        let dir = Directory::new();
        let store = Store::open(&dir.0).unwrap();
        let query = AlaSql::new("missing-node".into(), "missing-helper".into(), false);
        let handler = NioPgHandler::new(
            Arc::new(Mutex::new(store)),
            Arc::new(query),
            "ws_test".into(),
        );
        (dir, handler)
    }

    #[tokio::test]
    async fn test_pg_session_and_show() {
        let (_dir, handler) = test_handler();

        let resp = handler.execute_query("BEGIN").await.unwrap();
        assert!(matches!(resp, Response::Execution(_)));

        let resp = handler.execute_query("SET client_encoding = 'UTF8'").await.unwrap();
        assert!(matches!(resp, Response::Execution(_)));

        let resp = handler.execute_query("SHOW search_path").await.unwrap();
        if let Response::Query(q) = resp {
            assert_eq!(q.row_schema[0].name(), "search_path");
        } else {
            panic!("expected query response");
        }
    }

    #[tokio::test]
    async fn test_pg_builtins_and_catalogs() {
        let (_dir, handler) = test_handler();

        let resp = handler.execute_query("SELECT version()").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));

        let resp = handler.execute_query("SELECT current_schema()").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));

        let resp = handler.execute_query("SELECT current_database()").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));

        let resp = handler.execute_query("SELECT * FROM pg_namespace").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));

        let resp = handler.execute_query("SELECT * FROM pg_tables").await.unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }

    #[tokio::test]
    async fn test_pg_data_queries() {
        let (_dir, handler) = test_handler();

        {
            let mut store = handler.store.lock().unwrap();
            store
                .create(
                    "ws_test",
                    "customers".into(),
                    json!({"name": "Alice", "credits": 100, "active": true})
                        .as_object()
                        .unwrap()
                        .clone(),
                    None,
                )
                .unwrap();
            store
                .create(
                    "ws_test",
                    "customers".into(),
                    json!({"name": "Bob", "credits": 200, "active": false})
                        .as_object()
                        .unwrap()
                        .clone(),
                    None,
                )
                .unwrap();
        }

        let resp = handler
            .execute_query("SELECT name, credits, active FROM customers ORDER BY credits DESC")
            .await
            .unwrap();

        if let Response::Query(q) = resp {
            assert_eq!(q.row_schema.len(), 3);
            assert_eq!(q.row_schema[0].name(), "name");
            assert_eq!(q.row_schema[1].name(), "credits");
            assert_eq!(q.row_schema[2].name(), "active");
        } else {
            panic!("expected query response");
        }
    }

    #[test]
    fn test_pg_password_verification() {
        use sha2::{Digest, Sha256};
        let token = "niodb_client_1234567890abcdef1234567890abcdef";
        let token_hash = crate::storage::hex(&Sha256::digest(token.as_bytes()));
        let principals = Arc::new(vec![Principal {
            name: "test-user".into(),
            token_sha256: token_hash,
            workspaces: vec!["ws_test".into()],
            nio_skills: vec![],
            nio_plugins: vec![],
        }]);

        // 1. With explicit pg_password
        let auth = NioPgStartupHandler::new(principals.clone(), Some("supersecret".into()));
        assert!(auth.verify_password("supersecret"));
        assert!(auth.verify_password(token)); // valid token also works
        assert!(!auth.verify_password("wrongpass"));
        assert!(!auth.verify_password(""));

        // 2. Without explicit pg_password (requires valid token)
        let auth_token_only = NioPgStartupHandler::new(principals, None);
        assert!(auth_token_only.verify_password(token));
        assert!(!auth_token_only.verify_password("supersecret"));
        assert!(!auth_token_only.verify_password("wrongpass"));
        assert!(!auth_token_only.verify_password(""));
    }
}

