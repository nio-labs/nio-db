use niodb::{
    api::{self, App},
    auth,
    nio::Nio,
    query::AlaSql,
    storage::Store,
};
use std::{
    collections::BTreeSet,
    env,
    io::IsTerminal,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("niodb: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1).peekable();
    let mut data = PathBuf::from(env::var_os("NIODB_DIR").unwrap_or_else(|| "nio-db".into()));
    let mut listen: SocketAddr = env::var("NIODB_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7432".into())
        .parse()?;
    let mut pg_listen: Option<SocketAddr> = env::var("NIODB_PG_LISTEN")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| "127.0.0.1:5433".parse().ok());
    let mut pg_password = env::var("NIODB_PG_PASSWORD").ok();
    let mut auth_file = env::var_os("NIODB_AUTH_FILE").map(PathBuf::from);
    let mut ledger_checkpoint = env::var_os("NIODB_LEDGER_CHECKPOINT").map(PathBuf::from);
    let mut executable =
        PathBuf::from(env::var_os("NIODB_NIO_BIN").unwrap_or_else(|| "nio".into()));
    let mut timeout = 60u64;
    let mut init = false;
    let mut add_secret = false;
    let mut backup = false;
    let mut include_files = false;
    let mut seed_demo = env::var("NIODB_SEED_DEMO").as_deref() == Ok("1");
    let mut no_demo = env::var("NIODB_NO_DEMO").as_deref() == Ok("1");
    let mut output = None;
    let mut node = PathBuf::from(env::var_os("NIODB_NODE_BIN").unwrap_or_else(|| "node".into()));
    let mut helper = PathBuf::from(
        env::var_os("NIODB_ALASQL_HELPER")
            .unwrap_or_else(|| concat!(env!("CARGO_MANIFEST_DIR"), "/runtime/alasql.cjs").into()),
    );
    let mut name = "nio".to_string();
    let mut workspaces = Vec::new();
    let mut skills = Vec::new();
    let mut plugins = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "NioDB — The Agentic DB that works.\nA lightweight database with natural-language queries, powered by Nio.\n\nUsage: nio-db [serve|init-auth|add-secret|backup] [OPTIONS]\n\n  --dir PATH             Server data directory (default: nio-db)\n  --listen IP:PORT       Listen address (default: 127.0.0.1:7432)\n  --pg-listen IP:PORT    PostgreSQL wire protocol listen address (default: 127.0.0.1:5433)\n  --pg-password PASS     PostgreSQL password (default: secret token from data dir)\n  --no-pg                Disable PostgreSQL wire protocol connector\n  --auth-file PATH       Hashed bearer credentials (default: DIR/auth.json)\n  --ledger-checkpoint PATH  Durable ledger head outside DIR (NIODB_LEDGER_CHECKPOINT)\n  --nio-bin PATH         Nio CLI executable (default: nio on PATH)\n  --nio-timeout SECONDS  Timeout per Nio invocation (default: 60)\n  --node-bin PATH        Node executable for AlaSQL (default: node on PATH)\n  --alasql-helper PATH   AlaSQL helper script\n  --name NAME            Principal name for init-auth (default: nio)\n  --skill NAME           Nio skill grant for init-auth; repeatable\n  --plugin NAME          Nio plugin discovery grant; repeatable\n  --output PATH          New backup destination; stop the server before backup\n  --include-files        Back up journal and blobs into a new directory\n  --seed-demo            Add starter examples to an existing database once\n  --no-demo              Skip starter examples on first launch\n  --version              Print version\n\ninit-auth creates client and secret bearer tokens and prints both once.\nadd-secret adds or rotates the secret token; save its output privately.\nThe server invokes Nio for read-only natural-language assistance."
                );
                return Ok(());
            }
            "--version" | "-V" => {
                println!("niodb {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "serve" => {}
            "init-auth" => init = true,
            "add-secret" => add_secret = true,
            "backup" => backup = true,
            "--include-files" => include_files = true,
            "--seed-demo" => seed_demo = true,
            "--no-demo" => no_demo = true,
            "--output" => {
                output = Some(PathBuf::from(
                    args.next().ok_or("--output requires a path")?,
                ))
            }
            "--node-bin" => node = PathBuf::from(args.next().ok_or("--node-bin requires a path")?),
            "--alasql-helper" => {
                helper = PathBuf::from(args.next().ok_or("--alasql-helper requires a path")?)
            }
            "--dir" => data = PathBuf::from(args.next().ok_or("--dir requires a path")?),
            "--ledger-checkpoint" => {
                ledger_checkpoint = Some(PathBuf::from(args.next().ok_or("--ledger-checkpoint requires a path")?));
            }
            "--listen" => listen = args.next().ok_or("--listen requires an address")?.parse()?,
            "--pg-listen" => {
                pg_listen = Some(args.next().ok_or("--pg-listen requires an address")?.parse()?)
            }
            "--pg-password" => {
                pg_password = Some(args.next().ok_or("--pg-password requires a password")?)
            }
            "--no-pg" => pg_listen = None,
            "--auth-file" => {
                auth_file = Some(PathBuf::from(
                    args.next().ok_or("--auth-file requires a path")?,
                ))
            }
            "--nio-bin" => {
                executable = PathBuf::from(args.next().ok_or("--nio-bin requires a path")?)
            }
            "--nio-timeout" => {
                timeout = args
                    .next()
                    .ok_or("--nio-timeout requires seconds")?
                    .parse()?
            }
            "--name" => name = args.next().ok_or("--name requires a name")?,
            "--workspace" => workspaces.push(args.next().ok_or("--workspace requires an ID")?),
            "--skill" => skills.push(args.next().ok_or("--skill requires a name")?),
            "--plugin" => plugins.push(args.next().ok_or("--plugin requires a name")?),
            _ => return Err(format!("unknown argument {arg:?}; run nio-db --help").into()),
        }
    }
    if !(1..=300).contains(&timeout) {
        return Err("--nio-timeout must be between 1 and 300 seconds".into());
    }
    if seed_demo && no_demo {
        return Err("choose --seed-demo or --no-demo".into());
    }
    let auth_file = auth_file.unwrap_or_else(|| data.join("auth.json"));
    if [init, add_secret, backup]
        .into_iter()
        .filter(|selected| *selected)
        .count()
        > 1
    {
        return Err("choose init-auth, add-secret or backup".into());
    }
    if add_secret {
        println!("{}", auth::add_secret(&auth_file)?);
        return Ok(());
    }
    if backup {
        let destination = output.ok_or("backup requires --output PATH")?;
        let store = Store::open_with_checkpoint(&data, ledger_checkpoint.as_deref())?;
        if include_files {
            store.backup_full(&destination)?;
        } else {
            store.backup(&destination)?;
        }
        eprintln!("Backup saved to {}", destination.display());
        return Ok(());
    }
    if init {
        if workspaces.is_empty() {
            let existing = if data.join("journal.jsonl").exists() {
                Store::open_with_checkpoint(&data, ledger_checkpoint.as_deref())?.single_workspace()?
            } else {
                None
            };
            workspaces.push(existing.unwrap_or_else(|| "default".into()));
        }
        if workspaces.len() != 1 {
            return Err("NioDB uses one workspace per server".into());
        }
        println!(
            "{}",
            serde_json::to_string(&auth::init_pair_with_capabilities(
                &auth_file, name, workspaces, skills, plugins
            )?)?
        );
        return Ok(());
    }
    if executable.components().count() > 1 {
        executable = executable.canonicalize()?;
    }
    let mut principals = auth::load(&auth_file)
        .map_err(|_| "configure credentials with nio-db init-auth, or set --auth-file")?;
    let scopes: BTreeSet<_> = principals.iter().flat_map(|p| &p.workspaces).collect();
    if scopes.len() != 1 {
        return Err("NioDB uses one workspace per server. Configure credentials for one existing workspace; stored data is not modified.".into());
    }
    let mut store = Store::open_with_checkpoint(&data, ledger_checkpoint.as_deref())?;
    if let Some(scope) = store.single_workspace()? {
        for principal in &mut principals {
            principal.workspaces = vec![scope.clone()];
        }
    }
    if node.components().count() > 1 {
        node = node.canonicalize()?;
    }
    if helper.exists() {
        helper = helper.canonicalize()?;
    }
    let mut worker_runner =
        PathBuf::from(env::var_os("NIODB_WORKER_RUNNER").unwrap_or_else(|| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/runtime/event-worker.cjs").into()
        }));
    if worker_runner.exists() {
        worker_runner = worker_runner.canonicalize()?;
    }
    let query = AlaSql::detect(node.clone(), helper).await;
    let nio = Nio::detect(
        executable,
        data.canonicalize()?.join("nio-runtime"),
        Duration::from_secs(timeout),
    )
    .await?;
    let listener = tokio::net::TcpListener::bind(listen).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::AddrInUse {
            std::io::Error::new(error.kind(), format!("Port {} is already in use. Stop the other server, or choose another port with --listen 127.0.0.1:7433.", listen.port()))
        } else {
            error
        }
    })?;
    let mut address = listener.local_addr()?;
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    let url = format!("http://{address}");
    let demo_event = if no_demo { None } else {
        api::seed_default_demo(&mut store, &data, &principals[0].workspaces[0], &principals[0].name, &url, seed_demo)?
    };
    let app = App {
        store: Arc::new(Mutex::new(store)),
        principals: Arc::new(principals),
        nio: Arc::new(nio),
        query: Arc::new(query),
        conversations: Arc::new(Mutex::new(BTreeSet::new())),
        events: tokio::sync::broadcast::channel(256).0,
        node_binary: node,
        worker_runner,
    };
    if pg_password.is_none() {
        let secret_file = data.join("secret-token");
        if let Ok(token) = std::fs::read_to_string(&secret_file) {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                pg_password = Some(trimmed.to_string());
            }
        }
    }
    let pg_url = if let Some(pg_addr) = pg_listen {
        let app_pg = app.clone();
        let pass = pg_password.clone();
        tokio::spawn(async move {
            if let Err(e) = niodb::pg::run_pg_server(app_pg, pg_addr, pass).await {
                eprintln!("PostgreSQL connector error on {pg_addr}: {e}");
            }
        });
        Some(format!("postgresql://{pg_addr}"))
    } else {
        None
    };
    startup_banner(&app, &url, &data, &auth_file, pg_url.as_deref(), pg_password.as_deref());
    if let Some(event) = demo_event {
        api::dispatch_demo_event(app.clone(), event);
    }
    axum::serve(listener, api::router(app))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

fn startup_banner(
    app: &App,
    url: &str,
    data: &std::path::Path,
    auth_file: &std::path::Path,
    pg_url: Option<&str>,
    pg_password: Option<&str>,
) {
    eprintln!(
        "\n  {}",
        terminal_style(&format!("NioDB v{}", env!("CARGO_PKG_VERSION")), "1")
    );
    eprintln!("  The Agentic DB that works.");
    eprintln!("  A lightweight database with natural-language queries, powered by Nio.\n");
    eprintln!("  {}\n", terminal_style("Server is running", "32"));
    eprintln!("  Database   {}", terminal_style("Ready", "32"));
    eprintln!(
        "  SQL        {}",
        terminal_style(
            if app.query.ready {
                "Ready"
            } else {
                "Unavailable"
            },
            if app.query.ready { "32" } else { "33" }
        )
    );
    let nio_status = match app.nio.readiness.status.as_str() {
        "ready" => {
            if let Some(v) = &app.nio.readiness.version {
                if v.starts_with("fallback:") {
                    "LLM Fallback"
                } else {
                    "Configured"
                }
            } else {
                "Configured"
            }
        }
        "missing" => "Not installed",
        "unconfigured" => "Needs a model",
        "incompatible" => "Needs an update",
        _ => "Unavailable",
    };
    eprintln!(
        "  Nio        {}\n",
        terminal_style(
            nio_status,
            if app.nio.readiness.status == "ready" {
                "32"
            } else {
                "33"
            }
        )
    );
    eprintln!("  Console    {}", terminal_url(&format!("{url}/console")));
    eprintln!("  API docs   {}", terminal_url(&format!("{url}/doc")));
    eprintln!("  Guides     {}", terminal_url(&format!("{url}/guide")));
    eprintln!("  Server     {}", terminal_url(url));
    if let Some(pg) = pg_url {
        let pass_info = if let Some(p) = pg_password {
            format!("Password: {}", p)
        } else {
            "Password: auth token or --pg-password".to_string()
        };
        eprintln!(
            "  Postgres   {} (User: niodb, {})",
            terminal_url(pg),
            pass_info
        );
    }
    eprintln!(
        "  OpenAPI    {}\n",
        terminal_url(&format!("{url}/openapi.yaml"))
    );
    eprintln!("  Data       {}", terminal_path(data));
    let demo_login = data.join("demo-user.json");
    if demo_login.is_file() {
        eprintln!("  Demo login {}", terminal_path(&demo_login));
    }
    let client_token = data.join("client-token");
    let secret_token = data.join("secret-token");
    if auth_file == data.join("auth.json") && client_token.is_file() {
        eprintln!("  Client     {}", terminal_path(&client_token));
        if secret_token.is_file() {
            eprintln!("  Secret     {}", terminal_path(&secret_token));
        }
        eprintln!("\n  To try the API, open API docs and click Authorize.");
        eprintln!("  Paste your token (combination: client-token:secret-token, or client token).");
    } else {
        eprintln!("\n  Open API docs and use your bearer token to authorize requests.");
    }
    if !app.query.ready {
        eprintln!(
            "\n  SQL needs AlaSQL. Run npm install in the NioDB package folder, then restart."
        );
    }
    if app.nio.readiness.status != "ready" {
        eprintln!("\n  Natural-language queries need a configured Nio provider and model.");
        eprintln!("  Complete Nio setup, then restart this server.");
    }
    eprintln!("\n  {}\n", terminal_style("Press Ctrl+C to stop.", "2"));
}

fn styled_terminal() -> bool {
    std::io::stderr().is_terminal() && env::var("TERM").as_deref() != Ok("dumb")
}

fn terminal_style(text: &str, style: &str) -> String {
    if styled_terminal() && env::var_os("NO_COLOR").is_none() {
        format!("\x1b[{style}m{text}\x1b[0m")
    } else {
        text.to_owned()
    }
}

fn terminal_path(path: &std::path::Path) -> String {
    let absolute = path.canonicalize().unwrap_or_else(|_| path.to_owned());
    let display = env::current_dir()
        .ok()
        .and_then(|cwd| {
            absolute
                .strip_prefix(cwd)
                .ok()
                .map(|p| format!("./{}", p.display()))
        })
        .unwrap_or_else(|| absolute.display().to_string());
    if !styled_terminal() {
        return display;
    }
    let normalized = absolute.to_string_lossy().replace('\\', "/");
    let mut encoded = String::new();
    for byte in normalized.bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~:".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    let prefix = if cfg!(windows) { "file:///" } else { "file://" };
    format!("\x1b]8;;{prefix}{encoded}\x1b\\{display}\x1b]8;;\x1b\\")
}

fn terminal_url(url: &str) -> String {
    if styled_terminal() {
        format!("\x1b]8;;{url}\x1b\\{url}\x1b]8;;\x1b\\")
    } else {
        url.to_owned()
    }
}

async fn shutdown() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = interrupt => {}, _ = terminate => {} }
}
