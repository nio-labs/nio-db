use crate::storage::Artifact;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value, json};
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
        // Probe fallback availability, then release the process. Native SQL
        // needs no Node worker; unsupported queries start and reuse workers.
        if let Ok(Ok(mut worker)) = tokio::time::timeout(Duration::from_secs(5), engine.spawn()).await {
            let _ = worker.child.kill().await;
            engine.ready = true;
        }
        engine
    }

    pub async fn execute(&self, sql: String, parameters: Vec<Value>, artifacts: &[Arc<Artifact>], include_count: bool) -> Result<Value, Failure> {
        if artifacts.len() > 500_000 || sql.len() > 65536 { return Err(Failure::TooLarge); }
        // Execute natively in Rust with zero IPC whenever compatible
        match execute_native_sql(&sql, &parameters, artifacts, include_count) {
            Ok(result) => return Ok(result),
            Err(NativeSqlError::QueryRejected) => return Err(Failure::Rejected),
            Err(NativeSqlError::Unsupported) => {
                // Fall back to AlaSQL out-of-process engine
            }
        }

        if !self.ready { return Err(Failure::Unavailable); }

        {
            let mut idle_guard = self.idle.lock().await;
            if let Some(worker) = idle_guard.last_mut() {
                if let Ok(Some(_)) = worker.child.try_wait() {
                    let _ = idle_guard.pop();
                    drop(idle_guard);
                    self.replace_failed_worker();
                    return Err(Failure::Unavailable);
                }
            }
        }

        let _permit = self.capacity.try_acquire().map_err(|_| Failure::Busy)?;
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

// ---------------------------------------------------------------------------
// Native Rust Pure SQL Engine (Zero IPC, Microsecond Latency, Sub-MiB Footprint)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NativeSqlError {
    QueryRejected,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq)]
enum SqlToken {
    Select,
    From,
    Where,
    Group,
    By,
    Order,
    Limit,
    As,
    And,
    Or,
    Not,
    Is,
    Null,
    True,
    False,
    Asc,
    Desc,
    Count,
    Avg,
    Sum,
    Min,
    Max,
    In,
    Like,
    Asterisk,
    Comma,
    LParen,
    RParen,
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    Param(usize),
    Ident(String),
    StringLit(String),
    NumberLit(f64),
}

fn tokenize_sql(sql: &str) -> Result<Vec<SqlToken>, NativeSqlError> {
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut tokens = Vec::new();
    let mut param_counter = 1;

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '*' {
            tokens.push(SqlToken::Asterisk);
            i += 1;
            continue;
        }
        if c == ',' {
            tokens.push(SqlToken::Comma);
            i += 1;
            continue;
        }
        if c == '(' {
            tokens.push(SqlToken::LParen);
            i += 1;
            continue;
        }
        if c == ')' {
            tokens.push(SqlToken::RParen);
            i += 1;
            continue;
        }
        if c == '=' {
            tokens.push(SqlToken::Eq);
            i += 1;
            continue;
        }
        if c == '!' && i + 1 < chars.len() && chars[i + 1] == '=' {
            tokens.push(SqlToken::NotEq);
            i += 2;
            continue;
        }
        if c == '<' {
            if i + 1 < chars.len() && chars[i + 1] == '>' {
                tokens.push(SqlToken::NotEq);
                i += 2;
                continue;
            } else if i + 1 < chars.len() && chars[i + 1] == '=' {
                tokens.push(SqlToken::Lte);
                i += 2;
                continue;
            } else {
                tokens.push(SqlToken::Lt);
                i += 1;
                continue;
            }
        }
        if c == '>' {
            if i + 1 < chars.len() && chars[i + 1] == '=' {
                tokens.push(SqlToken::Gte);
                i += 2;
                continue;
            } else {
                tokens.push(SqlToken::Gt);
                i += 1;
                continue;
            }
        }
        if c == '?' {
            tokens.push(SqlToken::Param(param_counter));
            param_counter += 1;
            i += 1;
            continue;
        }
        if c == '$' {
            i += 1;
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if start == i {
                return Err(NativeSqlError::Unsupported);
            }
            let num: usize = sql[start..i].parse().map_err(|_| NativeSqlError::Unsupported)?;
            tokens.push(SqlToken::Param(num));
            continue;
        }
        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < chars.len() {
                if chars[i] == quote {
                    if i + 1 < chars.len() && chars[i + 1] == quote {
                        s.push(quote);
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    s.push(chars[i]);
                    i += 1;
                }
            }
            tokens.push(SqlToken::StringLit(s));
            continue;
        }
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) {
            let start = i;
            if c == '-' {
                i += 1;
            }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num_str: String = chars[start..i].iter().collect();
            let num: f64 = num_str.parse().map_err(|_| NativeSqlError::Unsupported)?;
            tokens.push(SqlToken::NumberLit(num));
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let upper = word.to_uppercase();
            let tok = match upper.as_str() {
                "SELECT" => SqlToken::Select,
                "FROM" => SqlToken::From,
                "WHERE" => SqlToken::Where,
                "GROUP" => SqlToken::Group,
                "BY" => SqlToken::By,
                "ORDER" => SqlToken::Order,
                "LIMIT" => SqlToken::Limit,
                "AS" => SqlToken::As,
                "AND" => SqlToken::And,
                "OR" => SqlToken::Or,
                "NOT" => SqlToken::Not,
                "IS" => SqlToken::Is,
                "NULL" => SqlToken::Null,
                "TRUE" => SqlToken::True,
                "FALSE" => SqlToken::False,
                "ASC" => SqlToken::Asc,
                "DESC" => SqlToken::Desc,
                "COUNT" => SqlToken::Count,
                "AVG" => SqlToken::Avg,
                "SUM" => SqlToken::Sum,
                "MIN" => SqlToken::Min,
                "MAX" => SqlToken::Max,
                "IN" => SqlToken::In,
                "LIKE" => SqlToken::Like,
                _ => SqlToken::Ident(word),
            };
            tokens.push(tok);
            continue;
        }
        return Err(NativeSqlError::Unsupported);
    }
    Ok(tokens)
}

#[derive(Debug, Clone)]
enum SelectExpr {
    Wildcard,
    Field { name: String, alias: Option<String> },
    Aggregate { func: AggFunc, field: Option<String>, alias: Option<String> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggFunc {
    Count,
    Avg,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone)]
enum SqlExpr {
    Binary { left: Box<SqlExpr>, op: BinaryOp, right: Box<SqlExpr> },
    Unary { op: UnaryOp, expr: Box<SqlExpr> },
    InList { expr: Box<SqlExpr>, list: Vec<SqlExpr>, negated: bool },
    Field(String),
    Literal(Value),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BinaryOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Like,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone)]
struct OrderByClause {
    field: String,
    descending: bool,
}

#[derive(Debug, Clone)]
struct ParsedSql {
    selects: Vec<SelectExpr>,
    from: String,
    where_expr: Option<SqlExpr>,
    group_by: Vec<String>,
    order_by: Vec<OrderByClause>,
    limit: Option<usize>,
}

struct SqlParser {
    tokens: Vec<SqlToken>,
    pos: usize,
    parameters: Vec<Value>,
}

impl SqlParser {
    fn new(tokens: Vec<SqlToken>, parameters: Vec<Value>) -> Self {
        Self { tokens, pos: 0, parameters }
    }

    fn peek(&self) -> Option<&SqlToken> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<SqlToken> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &SqlToken) -> Result<(), NativeSqlError> {
        match self.advance() {
            Some(ref tok) if tok == expected => Ok(()),
            _ => Err(NativeSqlError::Unsupported),
        }
    }

    fn parse_select_query(&mut self) -> Result<ParsedSql, NativeSqlError> {
        self.expect(&SqlToken::Select)?;
        let mut selects = Vec::new();
        loop {
            match self.peek() {
                Some(SqlToken::Asterisk) => {
                    self.advance();
                    selects.push(SelectExpr::Wildcard);
                }
                Some(SqlToken::Count) | Some(SqlToken::Avg) | Some(SqlToken::Sum) | Some(SqlToken::Min) | Some(SqlToken::Max) => {
                    let func_tok = self.advance().unwrap();
                    let func = match func_tok {
                        SqlToken::Count => AggFunc::Count,
                        SqlToken::Avg => AggFunc::Avg,
                        SqlToken::Sum => AggFunc::Sum,
                        SqlToken::Min => AggFunc::Min,
                        SqlToken::Max => AggFunc::Max,
                        _ => unreachable!(),
                    };
                    self.expect(&SqlToken::LParen)?;
                    let field = match self.peek() {
                        Some(SqlToken::Asterisk) => {
                            self.advance();
                            None
                        }
                        Some(SqlToken::Ident(_)) => {
                            if let Some(SqlToken::Ident(name)) = self.advance() {
                                Some(name)
                            } else {
                                None
                            }
                        }
                        _ => return Err(NativeSqlError::Unsupported),
                    };
                    self.expect(&SqlToken::RParen)?;
                    let alias = self.parse_optional_alias();
                    selects.push(SelectExpr::Aggregate { func, field, alias });
                }
                Some(SqlToken::Ident(_)) => {
                    let name = if let Some(SqlToken::Ident(n)) = self.advance() { n } else { unreachable!() };
                    let alias = self.parse_optional_alias();
                    selects.push(SelectExpr::Field { name, alias });
                }
                _ => return Err(NativeSqlError::Unsupported),
            }

            if self.peek() == Some(&SqlToken::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(&SqlToken::From)?;
        let from = match self.advance() {
            Some(SqlToken::Ident(tbl)) => tbl,
            _ => return Err(NativeSqlError::Unsupported),
        };

        let mut where_expr = None;
        if self.peek() == Some(&SqlToken::Where) {
            self.advance();
            where_expr = Some(self.parse_expr()?);
        }

        let mut group_by = Vec::new();
        if self.peek() == Some(&SqlToken::Group) {
            self.advance();
            self.expect(&SqlToken::By)?;
            loop {
                match self.advance() {
                    Some(SqlToken::Ident(col)) => group_by.push(col),
                    _ => return Err(NativeSqlError::Unsupported),
                }
                if self.peek() == Some(&SqlToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let mut order_by = Vec::new();
        if self.peek() == Some(&SqlToken::Order) {
            self.advance();
            self.expect(&SqlToken::By)?;
            loop {
                let field = match self.advance() {
                    Some(SqlToken::Ident(col)) => col,
                    _ => return Err(NativeSqlError::Unsupported),
                };
                let descending = if self.peek() == Some(&SqlToken::Desc) {
                    self.advance();
                    true
                } else if self.peek() == Some(&SqlToken::Asc) {
                    self.advance();
                    false
                } else {
                    false
                };
                order_by.push(OrderByClause { field, descending });
                if self.peek() == Some(&SqlToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let mut limit = None;
        if self.peek() == Some(&SqlToken::Limit) {
            self.advance();
            match self.advance() {
                Some(SqlToken::NumberLit(n)) => limit = Some(n as usize),
                Some(SqlToken::Param(idx)) => {
                    if let Some(val) = self.parameters.get(idx.saturating_sub(1)) {
                        limit = val.as_u64().map(|v| v as usize);
                    }
                }
                _ => return Err(NativeSqlError::Unsupported),
            }
        }

        Ok(ParsedSql { selects, from, where_expr, group_by, order_by, limit })
    }

    fn parse_optional_alias(&mut self) -> Option<String> {
        if self.peek() == Some(&SqlToken::As) {
            self.advance();
            match self.advance() {
                Some(SqlToken::Ident(alias)) | Some(SqlToken::StringLit(alias)) => Some(alias),
                _ => None,
            }
        } else if let Some(SqlToken::Ident(alias)) = self.peek().cloned() {
            // Check if next token is a keyword or comma/from
            if !matches!(alias.to_uppercase().as_str(), "FROM" | "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "AND" | "OR") {
                self.advance();
                Some(alias)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn parse_expr(&mut self) -> Result<SqlExpr, NativeSqlError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<SqlExpr, NativeSqlError> {
        let mut left = self.parse_and_expr()?;
        while self.peek() == Some(&SqlToken::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = SqlExpr::Binary { left: Box::new(left), op: BinaryOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<SqlExpr, NativeSqlError> {
        let mut left = self.parse_comparison_expr()?;
        while self.peek() == Some(&SqlToken::And) {
            self.advance();
            let right = self.parse_comparison_expr()?;
            left = SqlExpr::Binary { left: Box::new(left), op: BinaryOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_comparison_expr(&mut self) -> Result<SqlExpr, NativeSqlError> {
        let left = self.parse_primary_expr()?;

        if let Some(tok) = self.peek().cloned() {
            let op = match tok {
                SqlToken::Eq => Some(BinaryOp::Eq),
                SqlToken::NotEq => Some(BinaryOp::NotEq),
                SqlToken::Gt => Some(BinaryOp::Gt),
                SqlToken::Gte => Some(BinaryOp::Gte),
                SqlToken::Lt => Some(BinaryOp::Lt),
                SqlToken::Lte => Some(BinaryOp::Lte),
                SqlToken::Like => Some(BinaryOp::Like),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_primary_expr()?;
                return Ok(SqlExpr::Binary { left: Box::new(left), op, right: Box::new(right) });
            }

            if self.peek() == Some(&SqlToken::Is) {
                self.advance();
                if self.peek() == Some(&SqlToken::Not) {
                    self.advance();
                    self.expect(&SqlToken::Null)?;
                    return Ok(SqlExpr::Unary { op: UnaryOp::IsNotNull, expr: Box::new(left) });
                } else if self.peek() == Some(&SqlToken::Null) {
                    self.advance();
                    return Ok(SqlExpr::Unary { op: UnaryOp::IsNull, expr: Box::new(left) });
                }
            }

            if self.peek() == Some(&SqlToken::In) {
                self.advance();
                self.expect(&SqlToken::LParen)?;
                let mut list = Vec::new();
                loop {
                    list.push(self.parse_primary_expr()?);
                    if self.peek() == Some(&SqlToken::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.expect(&SqlToken::RParen)?;
                return Ok(SqlExpr::InList { expr: Box::new(left), list, negated: false });
            }
        }

        Ok(left)
    }

    fn parse_primary_expr(&mut self) -> Result<SqlExpr, NativeSqlError> {
        match self.advance() {
            Some(SqlToken::LParen) => {
                let expr = self.parse_expr()?;
                self.expect(&SqlToken::RParen)?;
                Ok(expr)
            }
            Some(SqlToken::StringLit(s)) => Ok(SqlExpr::Literal(Value::String(s))),
            Some(SqlToken::NumberLit(n)) => {
                if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                    Ok(SqlExpr::Literal(json!(n as i64)))
                } else {
                    Ok(SqlExpr::Literal(json!(n)))
                }
            }
            Some(SqlToken::True) => Ok(SqlExpr::Literal(Value::Bool(true))),
            Some(SqlToken::False) => Ok(SqlExpr::Literal(Value::Bool(false))),
            Some(SqlToken::Null) => Ok(SqlExpr::Literal(Value::Null)),
            Some(SqlToken::Param(idx)) => {
                if let Some(val) = self.parameters.get(idx.saturating_sub(1)) {
                    Ok(SqlExpr::Literal(val.clone()))
                } else {
                    Ok(SqlExpr::Literal(Value::Null))
                }
            }
            Some(SqlToken::Ident(name)) => Ok(SqlExpr::Field(name)),
            _ => Err(NativeSqlError::Unsupported),
        }
    }
}

#[derive(Clone)]
struct AggregateState {
    count: usize,
    sum: f64,
    integer_sum: Option<i64>,
    extreme: Option<Value>,
}

impl Default for AggregateState {
    fn default() -> Self {
        Self { count: 0, sum: 0.0, integer_sum: Some(0), extreme: None }
    }
}

impl AggregateState {
    fn push(&mut self, func: AggFunc, value: Option<&Value>, wildcard: bool) {
        match func {
            AggFunc::Count => {
                if wildcard || value.is_some_and(|v| !v.is_null()) { self.count += 1; }
            }
            AggFunc::Sum | AggFunc::Avg => {
                if let Some(value) = value {
                    if let Some(number) = value.as_f64() {
                        self.count += 1;
                        self.sum += number;
                        self.integer_sum = self.integer_sum.and_then(|sum|
                            value.as_i64().and_then(|n| sum.checked_add(n)));
                    }
                }
            }
            AggFunc::Min | AggFunc::Max => {
                if let Some(value) = value.filter(|v| !v.is_null()) {
                    let order = if func == AggFunc::Min { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
                    if self.extreme.as_ref().map_or(true, |current| cmp_sql_vals(value, current) == Some(order)) {
                        self.extreme = Some(value.clone());
                    }
                }
            }
        }
    }

    fn finish(self, func: AggFunc) -> Value {
        match func {
            AggFunc::Count => json!(self.count),
            AggFunc::Avg if self.count > 0 => json!(self.sum / self.count as f64),
            AggFunc::Sum if self.count > 0 => self.integer_sum.map_or_else(|| json!(self.sum), |n| json!(n)),
            AggFunc::Min | AggFunc::Max => self.extreme.unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }
}

pub fn execute_native_sql(
    sql: &str,
    parameters: &[Value],
    artifacts: &[Arc<Artifact>],
    include_count: bool,
) -> Result<Value, NativeSqlError> {
    let refs: Vec<&Arc<Artifact>> = artifacts.iter().collect();
    execute_native_sql_refs(sql, parameters, &refs, include_count)
}

static PARSED_SQL_CACHE: std::sync::LazyLock<std::sync::RwLock<std::collections::HashMap<String, Result<ParsedSql, NativeSqlError>>>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(std::collections::HashMap::new()));

fn get_parsed_query(sql: &str, parameters: &[Value]) -> Result<ParsedSql, NativeSqlError> {
    if parameters.is_empty() {
        if let Ok(cache) = PARSED_SQL_CACHE.read() {
            if let Some(res) = cache.get(sql) {
                return res.clone();
            }
        }
        let tokens = tokenize_sql(sql)?;
        let mut parser = SqlParser::new(tokens, Vec::new());
        let res = parser.parse_select_query();
        if let Ok(mut cache) = PARSED_SQL_CACHE.write() {
            cache.insert(sql.to_string(), res.clone());
        }
        res
    } else {
        let tokens = tokenize_sql(sql)?;
        let mut parser = SqlParser::new(tokens, parameters.to_vec());
        parser.parse_select_query()
    }
}

pub fn execute_native_sql_refs(
    sql: &str,
    parameters: &[Value],
    artifacts: &[&Arc<Artifact>],
    include_count: bool,
) -> Result<Value, NativeSqlError> {
    let parsed = get_parsed_query(sql, parameters)?;

    // Check table existence
    let is_all_records = parsed.from.eq_ignore_ascii_case("records") || parsed.from.eq_ignore_ascii_case("artifacts");
    let has_matching_collection = if is_all_records {
        true
    } else if let Some(first) = artifacts.first() {
        if first.kind.eq_ignore_ascii_case(&parsed.from) {
            true
        } else {
            artifacts.iter().any(|a| a.kind.eq_ignore_ascii_case(&parsed.from))
        }
    } else {
        false
    };
    if !is_all_records && !has_matching_collection && !artifacts.is_empty() {
        return Err(NativeSqlError::QueryRejected);
    }

    // Filter candidate artifacts
    let candidate_storage: Vec<&Arc<Artifact>>;
    let candidate_refs: &[&Arc<Artifact>] = if parsed.where_expr.is_none() && (is_all_records || artifacts.first().map(|a| a.kind.eq_ignore_ascii_case(&parsed.from)).unwrap_or(false)) {
        artifacts
    } else {
        candidate_storage = artifacts.iter().copied().filter(|a| {
            if !is_all_records && !a.kind.eq_ignore_ascii_case(&parsed.from) {
                return false;
            }
            if let Some(ref where_expr) = parsed.where_expr {
                eval_filter(where_expr, a, parameters)
            } else {
                true
            }
        }).collect();
        &candidate_storage
    };

    let has_aggregates = parsed.selects.iter().any(|s| matches!(s, SelectExpr::Aggregate { .. })) || !parsed.group_by.is_empty();

    let mut output_rows: Vec<Value> = Vec::new();

    if has_aggregates {
        let agg_descriptors: Vec<(usize, AggFunc, Option<&str>, bool)> = parsed
            .selects
            .iter()
            .enumerate()
            .filter_map(|(idx, select)| match select {
                SelectExpr::Aggregate { func, field, .. } => Some((idx, *func, field.as_deref(), field.is_none())),
                _ => None,
            })
            .collect();

        // Accumulate each aggregate as records arrive. Keep first-seen group
        // order for queries without ORDER BY and one representative for fields.
        let groups: Vec<(Vec<Value>, Option<&Arc<Artifact>>, Vec<AggregateState>)> = if parsed.group_by.len() == 1 {
            let group_field = &parsed.group_by[0];
            let mut g_list: Vec<(Vec<Value>, Option<&Arc<Artifact>>, Vec<AggregateState>)> = Vec::with_capacity(8);
            for &a in candidate_refs {
                let key_val = record_field_value(a, group_field);
                let index = if let Some(idx) = g_list.iter().position(|(k, ..)| k[0] == *key_val) {
                    idx
                } else {
                    let idx = g_list.len();
                    g_list.push((vec![key_val.into_owned()], None, vec![AggregateState::default(); parsed.selects.len()]));
                    idx
                };
                let (_, first, states) = &mut g_list[index];
                first.get_or_insert(a);
                for &(state_idx, func, field_opt, is_wildcard) in &agg_descriptors {
                    let val_ref = field_opt.and_then(|f| get_field_ref(a, f));
                    states[state_idx].push(func, val_ref, is_wildcard);
                }
            }
            g_list
        } else {
            let mut groups: Vec<(Vec<Value>, Option<&Arc<Artifact>>, Vec<AggregateState>)> = Vec::new();
            let mut group_index = std::collections::HashMap::new();
            if parsed.group_by.is_empty() {
                groups.push((Vec::new(), None, vec![AggregateState::default(); parsed.selects.len()]));
            }
            for &a in candidate_refs {
                let index = if parsed.group_by.is_empty() {
                    0
                } else {
                    let keys: Vec<_> = parsed.group_by.iter().map(|f| record_field_value(a, f)).collect();
                    if let Some(&index) = group_index.get(&keys) {
                        index
                    } else {
                        let index = groups.len();
                        let owned_keys = keys.iter().map(|key| key.as_ref().clone()).collect();
                        group_index.insert(keys, index);
                        groups.push((owned_keys, None, vec![AggregateState::default(); parsed.selects.len()]));
                        index
                    }
                };
                let (_, first, states) = &mut groups[index];
                first.get_or_insert(a);
                for (select, state) in parsed.selects.iter().zip(states) {
                    if let SelectExpr::Aggregate { func, field, .. } = select {
                        state.push(*func, field.as_deref().and_then(|f| get_field_ref(a, f)), field.is_none());
                    }
                }
            }
            groups
        };
        for (group_keys, first, states) in groups {
            let mut row = Map::new();
            for (select, state) in parsed.selects.iter().zip(states) {
                match select {
                    SelectExpr::Wildcard => {}
                    SelectExpr::Field { name, alias } => {
                        let value = if let Some(pos) = parsed.group_by.iter().position(|g| g.eq_ignore_ascii_case(name)) {
                            group_keys[pos].clone()
                        } else {
                            first.map(|a| get_record_field_val(a, name)).unwrap_or(Value::Null)
                        };
                        row.insert(alias.as_deref().unwrap_or(name).to_string(), value);
                    }
                    SelectExpr::Aggregate { func, field, alias } => {
                        let label = match func {
                            AggFunc::Count => "count", AggFunc::Avg => "avg", AggFunc::Sum => "sum",
                            AggFunc::Min => "min", AggFunc::Max => "max",
                        };
                        let key = alias.clone().unwrap_or_else(|| format!("{}({})", label,
                            field.as_deref().unwrap_or(if *func == AggFunc::Count { "*" } else { "" })));
                        row.insert(key, state.finish(*func));
                    }
                }
            }
            output_rows.push(Value::Object(row));
        }

        // Sorting (ORDER BY) for aggregate rows
        if !parsed.order_by.is_empty() {
            output_rows.sort_by(|row_a, row_b| {
                for ord in &parsed.order_by {
                    let v_a = row_a.get(&ord.field).unwrap_or(&Value::Null);
                    let v_b = row_b.get(&ord.field).unwrap_or(&Value::Null);
                    let cmp = cmp_sql_vals(v_a, v_b).unwrap_or(std::cmp::Ordering::Equal);
                    let res = if ord.descending { cmp.reverse() } else { cmp };
                    if res != std::cmp::Ordering::Equal {
                        return res;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(lim) = parsed.limit {
            output_rows.truncate(lim);
        }
    } else {
        // Row-by-row selection
        let mut sorted_refs = candidate_refs.to_vec();
        if !parsed.order_by.is_empty() {
            let single_int_sort = if parsed.order_by.len() == 1 {
                let ord = &parsed.order_by[0];
                let mut int_ranked: Vec<(i64, usize, &Arc<Artifact>)> = Vec::with_capacity(sorted_refs.len());
                let mut all_ints = true;
                for (idx, &a) in sorted_refs.iter().enumerate() {
                    if let Some(n) = get_field_ref(a, &ord.field).and_then(Value::as_i64) {
                        int_ranked.push((n, idx, a));
                    } else {
                        all_ints = false;
                        break;
                    }
                }
                if all_ints && int_ranked.len() == sorted_refs.len() {
                    let descending = ord.descending;
                    let cmp = |a: &(i64, usize, &Arc<Artifact>), b: &(i64, usize, &Arc<Artifact>)| {
                        let c = if descending { b.0.cmp(&a.0) } else { a.0.cmp(&b.0) };
                        c.then_with(|| a.1.cmp(&b.1))
                    };
                    if let Some(limit) = parsed.limit {
                        if limit < int_ranked.len() {
                            if limit > 0 {
                                int_ranked.select_nth_unstable_by(limit, cmp);
                            }
                            int_ranked.truncate(limit);
                        }
                    }
                    int_ranked.sort_unstable_by(cmp);
                    Some(int_ranked.into_iter().map(|(_, _, a)| a).collect())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(sorted) = single_int_sort {
                sorted_refs = sorted;
            } else {
                let mut ranked: Vec<_> = sorted_refs.into_iter().enumerate().collect();
                // Partial selection requires a total ordering. Preserve the old
                // stable-sort behavior for heterogeneous document field types.
                let comparable = parsed.order_by.iter().all(|ord| {
                    let mut category = None;
                    ranked.iter().all(|(_, a)| {
                        let value = record_field_value(a, &ord.field);
                        let current = match value.as_ref() {
                            Value::Null => return true,
                            Value::Number(_) => 0, Value::String(_) => 1, Value::Bool(_) => 2,
                            _ => return false,
                        };
                        if let Some(previous) = category { previous == current }
                        else { category = Some(current); true }
                    })
                });
                let compare_values = |(_, a): &(usize, &Arc<Artifact>), (_, b): &(usize, &Arc<Artifact>)| {
                    for ord in &parsed.order_by {
                        let va = record_field_value(a, &ord.field);
                        let vb = record_field_value(b, &ord.field);
                        let cmp = cmp_sql_vals(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                        let result = if ord.descending { cmp.reverse() } else { cmp };
                        if result != std::cmp::Ordering::Equal { return result; }
                    }
                    std::cmp::Ordering::Equal
                };
                let compare = |a: &(usize, &Arc<Artifact>), b: &(usize, &Arc<Artifact>)|
                    compare_values(a, b).then_with(|| a.0.cmp(&b.0));
                if comparable {
                    if let Some(limit) = parsed.limit {
                        if limit < ranked.len() {
                            if limit > 0 { ranked.select_nth_unstable_by(limit, compare); }
                            ranked.truncate(limit);
                        }
                    }
                    ranked.sort_unstable_by(compare);
                } else {
                    ranked.sort_by(compare_values);
                    if let Some(limit) = parsed.limit { ranked.truncate(limit); }
                }
                sorted_refs = ranked.into_iter().map(|(_, a)| a).collect();
            }
        } else if let Some(limit) = parsed.limit {
            sorted_refs.truncate(limit);
        }
        let is_wildcard = parsed.selects.iter().any(|s| matches!(s, SelectExpr::Wildcard));
        let field_selectors: Vec<(&str, &str)> = if !is_wildcard {
            parsed.selects.iter().filter_map(|s| match s {
                SelectExpr::Field { name, alias } => Some((alias.as_deref().unwrap_or(name.as_str()), name.as_str())),
                _ => None,
            }).collect()
        } else {
            Vec::new()
        };

        for a in sorted_refs {
            let mut row = Map::new();
            if is_wildcard {
                for (k, v) in &a.data {
                    if !["id", "type", "collection", "revision", "created_at", "updated_at"].contains(&k.as_str()) {
                        row.insert(k.clone(), v.clone());
                    }
                }
                row.insert("id".into(), Value::String(a.id.clone()));
                row.insert("type".into(), Value::String(a.kind.clone()));
                row.insert("collection".into(), Value::String(a.kind.clone()));
                row.insert("revision".into(), json!(a.revision));
                row.insert("created_at".into(), Value::String(a.created_at.clone()));
                row.insert("updated_at".into(), Value::String(a.updated_at.clone()));
            } else {
                for &(out_key, name) in &field_selectors {
                    row.insert(out_key.to_string(), get_record_field_val(a, name));
                }
            }
            output_rows.push(Value::Object(row));
        }
    }

    let total = output_rows.len();
    Ok(json!({
        "items": output_rows,
        "total": if include_count { Some(total) } else { None }
    }))
}

#[inline(always)]
fn get_field_ref<'a>(artifact: &'a Artifact, field: &str) -> Option<&'a Value> {
    match field {
        _ => artifact.data.get(field),
    }
}

// Document fields can be compared and grouped without cloning their JSON.
fn record_field_value<'a>(artifact: &'a Artifact, field: &str) -> std::borrow::Cow<'a, Value> {
    if matches!(field, "id" | "collection" | "type" | "revision" | "created_at" | "updated_at") {
        std::borrow::Cow::Owned(get_record_field_val(artifact, field))
    } else {
        std::borrow::Cow::Borrowed(artifact.data.get(field).unwrap_or(&Value::Null))
    }
}

fn get_record_field_val(artifact: &Artifact, field: &str) -> Value {
    match field {
        "id" => Value::String(artifact.id.clone()),
        "collection" | "type" => Value::String(artifact.kind.clone()),
        "revision" => json!(artifact.revision),
        "created_at" => Value::String(artifact.created_at.clone()),
        "updated_at" => Value::String(artifact.updated_at.clone()),
        _ => artifact.data.get(field).cloned().unwrap_or(Value::Null),
    }
}

fn eval_filter(expr: &SqlExpr, artifact: &Artifact, params: &[Value]) -> bool {
    match expr {
        SqlExpr::Binary { left, op: BinaryOp::Eq, right } => {
            if let (SqlExpr::Field(field), SqlExpr::Literal(lit)) = (left.as_ref(), right.as_ref()) {
                return get_field_ref(artifact, field).map_or(false, |v| vals_equal(v, lit));
            } else if let (SqlExpr::Literal(lit), SqlExpr::Field(field)) = (left.as_ref(), right.as_ref()) {
                return get_field_ref(artifact, field).map_or(false, |v| vals_equal(lit, v));
            }
            let l = eval_sql_expr(left, artifact, params);
            let r = eval_sql_expr(right, artifact, params);
            vals_equal(&l, &r)
        }
        SqlExpr::Binary { left, op: BinaryOp::NotEq, right } => {
            if let (SqlExpr::Field(field), SqlExpr::Literal(lit)) = (left.as_ref(), right.as_ref()) {
                return get_field_ref(artifact, field).map_or(true, |v| !vals_equal(v, lit));
            } else if let (SqlExpr::Literal(lit), SqlExpr::Field(field)) = (left.as_ref(), right.as_ref()) {
                return get_field_ref(artifact, field).map_or(true, |v| !vals_equal(lit, v));
            }
            let l = eval_sql_expr(left, artifact, params);
            let r = eval_sql_expr(right, artifact, params);
            !vals_equal(&l, &r)
        }
        SqlExpr::Binary { left, op: BinaryOp::And, right } => {
            eval_filter(left, artifact, params) && eval_filter(right, artifact, params)
        }
        SqlExpr::Binary { left, op: BinaryOp::Or, right } => {
            eval_filter(left, artifact, params) || eval_filter(right, artifact, params)
        }
        _ => {
            let res = eval_sql_expr(expr, artifact, params);
            val_is_truthy(&res)
        }
    }
}

fn eval_sql_expr(expr: &SqlExpr, artifact: &Artifact, params: &[Value]) -> Value {
    match expr {
        SqlExpr::Literal(val) => val.clone(),
        SqlExpr::Field(name) => get_record_field_val(artifact, name),
        SqlExpr::Unary { op, expr } => {
            let val = eval_sql_expr(expr, artifact, params);
            match op {
                UnaryOp::Not => Value::Bool(!val_is_truthy(&val)),
                UnaryOp::IsNull => Value::Bool(val.is_null()),
                UnaryOp::IsNotNull => Value::Bool(!val.is_null()),
            }
        }
        SqlExpr::InList { expr, list, negated } => {
            let val = eval_sql_expr(expr, artifact, params);
            let found = list.iter().any(|item| {
                let item_val = eval_sql_expr(item, artifact, params);
                vals_equal(&val, &item_val)
            });
            Value::Bool(if *negated { !found } else { found })
        }
        SqlExpr::Binary { left, op, right } => {
            match op {
                BinaryOp::And => {
                    let l = eval_sql_expr(left, artifact, params);
                    if !val_is_truthy(&l) {
                        return Value::Bool(false);
                    }
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(val_is_truthy(&r))
                }
                BinaryOp::Or => {
                    let l = eval_sql_expr(left, artifact, params);
                    if val_is_truthy(&l) {
                        return Value::Bool(true);
                    }
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(val_is_truthy(&r))
                }
                BinaryOp::Eq => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(vals_equal(&l, &r))
                }
                BinaryOp::NotEq => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(!vals_equal(&l, &r))
                }
                BinaryOp::Gt => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(cmp_sql_vals(&l, &r) == Some(std::cmp::Ordering::Greater))
                }
                BinaryOp::Gte => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(matches!(cmp_sql_vals(&l, &r), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)))
                }
                BinaryOp::Lt => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(cmp_sql_vals(&l, &r) == Some(std::cmp::Ordering::Less))
                }
                BinaryOp::Lte => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    Value::Bool(matches!(cmp_sql_vals(&l, &r), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))
                }
                BinaryOp::Like => {
                    let l = eval_sql_expr(left, artifact, params);
                    let r = eval_sql_expr(right, artifact, params);
                    if let (Some(text), Some(pattern)) = (l.as_str(), r.as_str()) {
                        Value::Bool(like_match(text, pattern))
                    } else {
                        Value::Bool(false)
                    }
                }
            }
        }
    }
}

fn val_is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Number(n) => n.as_f64().map_or(false, |x| x != 0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn vals_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                i1 == i2
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1 == f2
            } else {
                false
            }
        }
        (Value::String(s1), Value::String(s2)) => s1 == s2,
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        (Value::Null, Value::Null) => true,
        _ => a == b,
    }
}

fn cmp_sql_vals(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                Some(i1.cmp(&i2))
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1.partial_cmp(&f2)
            } else {
                None
            }
        }
        (Value::String(s1), Value::String(s2)) => Some(s1.cmp(s2)),
        (Value::Bool(b1), Value::Bool(b2)) => Some(b1.cmp(b2)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

fn like_match(text: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    let p_chars: Vec<char> = pattern.chars().collect();
    let t_chars: Vec<char> = text.chars().collect();
    let mut dp = vec![vec![false; t_chars.len() + 1]; p_chars.len() + 1];
    dp[0][0] = true;
    for i in 1..=p_chars.len() {
        if p_chars[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=p_chars.len() {
        for j in 1..=t_chars.len() {
            if p_chars[i - 1] == '%' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if p_chars[i - 1] == '_' || p_chars[i - 1].eq_ignore_ascii_case(&t_chars[j - 1]) {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    dp[p_chars.len()][t_chars.len()]
}

pub fn extract_table_from_sql(sql: &str) -> Option<String> {
    get_parsed_query(sql, &[]).ok().map(|p| p.from)
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
    async fn native_sql_does_not_require_an_available_fallback() {
        let engine = AlaSql::new("missing-node".into(), "missing-helper".into(), false);
        let result = engine.execute("SELECT id FROM records".into(), vec![], &[row("a")], false).await.unwrap();
        assert_eq!(result["items"], json!([{"id":"a"}]));
        assert_eq!(engine.execute("SELECT UPPER(name) FROM customers".into(), vec![], &[row("a")], false).await, Err(Failure::Unavailable));
    }

    #[test]
    fn native_aggregates_preserve_nulls_numeric_types_and_group_order() {
        let records: Vec<_> = [
            json!({"g":"b","x":2}), json!({"g":"a","x":null}),
            json!({"g":"b","x":4.5}), json!({"g":"a","x":"text"}),
        ].into_iter().enumerate().map(|(i, data)| {
            let mut artifact = row(&i.to_string()).as_ref().clone();
            artifact.data = data.as_object().unwrap().clone();
            Arc::new(artifact)
        }).collect();
        let query = "SELECT g, COUNT(*) AS n, COUNT(x) AS present, SUM(x) AS total, AVG(x) AS mean, MIN(x) AS lo, MAX(x) AS hi FROM customers GROUP BY g";
        let result = execute_native_sql(query, &[], &records, false).unwrap();
        assert_eq!(result["items"], json!([
            {"g":"b","n":2,"present":2,"total":6.5,"mean":3.25,"lo":2,"hi":4.5},
            {"g":"a","n":2,"present":1,"total":null,"mean":null,"lo":"text","hi":"text"}
        ]));
        let result = execute_native_sql("SELECT COUNT(*) AS n, SUM(x) AS total, AVG(x) AS mean FROM customers", &[], &[], false).unwrap();
        assert_eq!(result["items"], json!([{"n":0,"total":null,"mean":null}]));
        let integers: Vec<_> = records.into_iter().take(1).collect();
        let result = execute_native_sql("SELECT SUM(x) AS total FROM customers", &[], &integers, false).unwrap();
        assert!(result["items"][0]["total"].is_i64());
    }

    #[test]
    fn native_limited_sort_matches_full_sort_with_ties_and_multiple_keys() {
        let records: Vec<_> = (0..100).map(|i| {
            let mut artifact = row(&i.to_string()).as_ref().clone();
            artifact.data = json!({"name":i.to_string(),"credits":i%7,"other":i%3,"mixed":if i%2==0 { json!(i%7) } else { json!("x") }}).as_object().unwrap().clone();
            Arc::new(artifact)
        }).collect();
        for order in ["credits DESC", "credits ASC, other DESC", "mixed ASC"] {
            let sql = format!("SELECT name, credits FROM customers ORDER BY {order}");
            let full = execute_native_sql(&sql, &[], &records, false).unwrap();
            for limit in [0, 1, 5, 50, 100, 101] {
                let limited = execute_native_sql(&format!("{sql} LIMIT {limit}"), &[], &records, false).unwrap();
                assert_eq!(limited["items"].as_array().unwrap(),
                    &full["items"].as_array().unwrap()[..limit.min(records.len())]);
            }
        }
    }

    #[tokio::test]
    async fn workers_reuse_processes_and_do_not_retain_other_callers_rows() {
        let engine = AlaSql::detect("node".into(), PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/runtime/alasql.cjs"))).await;
        assert!(engine.ready);
        assert!(engine.idle.lock().await.is_empty());
        let native = engine.execute("SELECT id FROM records".into(), vec![], &[row("actual")], false).await.unwrap();
        assert_eq!(native["items"], json!([{"id":"actual"}]));
        assert!(engine.idle.lock().await.is_empty());
        let fallback_sql = "SELECT UPPER(name) AS upper_name FROM customers";
        let result = engine.execute(fallback_sql.into(), vec![], &[row("actual")], false).await.unwrap();
        assert_eq!(result["items"], json!([{"upper_name":"ADA"}]));
        let before: Vec<_> = engine.idle.lock().await.iter().map(|w| w.child.id()).collect();
        assert_eq!(before.len(), 1);
        for _ in 0..5 {
            let result = engine.execute("SELECT id, collection, name FROM customers".into(), vec![], &[row("actual")], false).await.unwrap();
            assert_eq!(result["items"], json!([{"id":"actual","collection":"customers","name":"Ada"}]));
            let result = engine.execute("SELECT UPPER(id) AS id FROM records".into(), vec![], &[], false).await.unwrap();
            assert_eq!(result["items"], json!([]));
        }
        assert_eq!(engine.execute("SELECT * FROM secrets".into(), vec![], &[row("a")], false).await, Err(Failure::Rejected));
        let after: Vec<_> = engine.idle.lock().await.iter().map(|w| w.child.id()).collect();
        assert_eq!(before, after);
        // A terminated worker is discarded, and following requests remain usable.
        engine.idle.lock().await.last_mut().unwrap().child.kill().await.unwrap();
        assert_eq!(engine.execute(fallback_sql.into(), vec![], &[row("a")], false).await, Err(Failure::Unavailable));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if engine.idle.lock().await.len() == 1 { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        assert!(engine.execute(fallback_sql.into(), vec![], &[row("a")], false).await.is_ok());
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
