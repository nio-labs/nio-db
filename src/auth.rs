use crate::storage::{hex, private_file, secure_dir, sync_dir};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    path::Path,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub name: String,
    pub token_sha256: String,
    pub workspaces: Vec<String>,
    #[serde(default)]
    pub nio_skills: Vec<String>,
    #[serde(default)]
    pub nio_plugins: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthFile {
    pub principals: Vec<Principal>,
}

#[derive(Serialize)]
pub struct TokenPair {
    pub client_token: String,
    pub secret_token: String,
}

fn new_token(prefix: &str) -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("niodb_{prefix}_{}", hex(&bytes))
}

fn valid_capabilities(skills: &[String], plugins: &[String]) -> bool {
    !skills.iter().chain(plugins).any(|name| {
        name.is_empty()
            || name.len() > 80
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

fn ensure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
        && !parent.exists()
    {
        secure_dir(parent)?;
    }
    Ok(())
}

fn write_new(path: &Path, auth: &AuthFile) -> io::Result<()> {
    ensure_parent(path)?;
    let mut handle = private_file(path, true)?;
    handle.write_all(&serde_json::to_vec_pretty(auth).map_err(io::Error::other)?)?;
    handle.sync_all()?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        sync_dir(parent)?;
    }
    Ok(())
}

fn replace(path: &Path, auth: &AuthFile) -> io::Result<()> {
    let mut random = [0u8; 8];
    OsRng.fill_bytes(&mut random);
    let temporary = path.with_extension(format!("{}.tmp", hex(&random)));
    let result = (|| {
        write_new(&temporary, auth)?;
        fs::rename(&temporary, path)?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

pub fn init_pair_with_capabilities(
    path: &Path,
    name: String,
    workspaces: Vec<String>,
    nio_skills: Vec<String>,
    nio_plugins: Vec<String>,
) -> io::Result<TokenPair> {
    if name.trim().is_empty()
        || workspaces.is_empty()
        || workspaces.iter().any(|w| w.trim().is_empty())
        || !valid_capabilities(&nio_skills, &nio_plugins)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid credential name, workspace or capability grants",
        ));
    }
    let client_token = new_token("client");
    let secret_token = new_token("secret");
    let client = Principal {
        name: name.clone(),
        token_sha256: hex(&Sha256::digest(client_token.as_bytes())),
        workspaces: workspaces.clone(),
        nio_skills: nio_skills.clone(),
        nio_plugins: nio_plugins.clone(),
    };
    let secret = Principal {
        name: format!("{name}-secret"),
        token_sha256: hex(&Sha256::digest(secret_token.as_bytes())),
        workspaces,
        nio_skills,
        nio_plugins,
    };
    write_new(
        path,
        &AuthFile {
            principals: vec![client, secret],
        },
    )?;
    Ok(TokenPair {
        client_token,
        secret_token,
    })
}

/// Adds or rotates the second full-access credential without changing the first.
pub fn add_secret(path: &Path) -> io::Result<String> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::other("credential file must be a regular file"));
    }
    let mut principals = load(path)?;
    let base = principals
        .first()
        .ok_or_else(|| io::Error::other("no base credential"))?
        .clone();
    let secret_name = format!("{}-secret", base.name);
    let secret_token = new_token("secret");
    let hash = hex(&Sha256::digest(secret_token.as_bytes()));
    if let Some(secret) = principals.iter_mut().find(|p| p.name == secret_name) {
        secret.token_sha256 = hash;
    } else {
        principals.push(Principal {
            name: secret_name,
            token_sha256: hash,
            workspaces: base.workspaces,
            nio_skills: base.nio_skills,
            nio_plugins: base.nio_plugins,
        });
    }
    replace(path, &AuthFile { principals })?;
    clear_auth_cache();
    Ok(secret_token)
}

pub fn load(path: &Path) -> io::Result<Vec<Principal>> {
    let contents = fs::read(path)?;
    let auth: AuthFile = serde_json::from_slice(&contents).map_err(io::Error::other)?;
    if auth.principals.is_empty()
        || auth.principals.iter().any(|p| {
            p.name.is_empty()
                || p.token_sha256.len() != 64
                || !p
                    .token_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                || p.workspaces.is_empty()
                || p.workspaces.iter().any(|w| w.trim().is_empty())
        })
    {
        return Err(io::Error::other(
            "auth file must contain valid principals and workspace grants",
        ));
    }
    let mut names = std::collections::HashSet::new();
    let mut hashes = std::collections::HashSet::new();
    if !auth
        .principals
        .iter()
        .all(|p| names.insert(&p.name) && hashes.insert(&p.token_sha256))
    {
        return Err(io::Error::other(
            "principal names and token hashes must be unique",
        ));
    }
    Ok(auth.principals)
}

fn authenticate_single(principals: &[Principal], bearer: &str) -> Option<Principal> {
    let digest = hex(&Sha256::digest(bearer.as_bytes()));
    principals
        .iter()
        .find(|p| {
            p.token_sha256.len() == digest.len()
                && p.token_sha256
                    .as_bytes()
                    .iter()
                    .zip(digest.as_bytes())
                    .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                    == 0
        })
        .cloned()
}

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

static AUTH_CACHE: LazyLock<RwLock<HashMap<String, Option<Principal>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn clear_auth_cache() {
    if let Ok(mut guard) = AUTH_CACHE.write() {
        guard.clear();
    }
}

pub fn authenticate(principals: &[Principal], bearer: &str) -> Option<Principal> {
    if let Ok(guard) = AUTH_CACHE.read() {
        if let Some(cached) = guard.get(bearer) {
            return cached.clone();
        }
    }
    let result = authenticate_uncached(principals, bearer);
    if let Ok(mut guard) = AUTH_CACHE.write() {
        if guard.len() < 1000 {
            guard.insert(bearer.to_string(), result.clone());
        }
    }
    result
}

fn authenticate_uncached(principals: &[Principal], bearer: &str) -> Option<Principal> {
    // If bearer is a combined token (client:secret or client.secret), verify both
    if let Some((t1, t2)) = bearer.split_once(':').or_else(|| bearer.split_once('.')) {
        if !t1.is_empty() && !t2.is_empty() {
            let p1 = authenticate_single(principals, t1);
            let p2 = authenticate_single(principals, t2);
            if let (Some(a1), Some(a2)) = (p1, p2) {
                if a1.workspaces == a2.workspaces {
                    let mut combined = a1.clone();
                    for s in &a2.nio_skills {
                        if !combined.nio_skills.contains(s) {
                            combined.nio_skills.push(s.clone());
                        }
                    }
                    for p in &a2.nio_plugins {
                        if !combined.nio_plugins.contains(p) {
                            combined.nio_plugins.push(p.clone());
                        }
                    }
                    return Some(combined);
                }
            }
        }
    }
    authenticate_single(principals, bearer)
}

/// Bootstrap only. Never replaces an existing credential file.
pub fn init(path: &Path, name: String, workspaces: Vec<String>) -> io::Result<String> {
    init_with_capabilities(path, name, workspaces, Vec::new(), Vec::new())
}

pub fn init_with_capabilities(
    path: &Path,
    name: String,
    workspaces: Vec<String>,
    nio_skills: Vec<String>,
    nio_plugins: Vec<String>,
) -> io::Result<String> {
    if name.trim().is_empty()
        || workspaces.is_empty()
        || workspaces.iter().any(|w| w.trim().is_empty())
    {
        return Err(io::Error::other(
            "name and at least one nonempty workspace are required",
        ));
    }
    if !valid_capabilities(&nio_skills, &nio_plugins) {
        return Err(io::Error::other(
            "capability names must be 1 to 80 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    ensure_parent(path)?;
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let token = format!("niodb_{}", hex(&bytes));
    let file = AuthFile {
        principals: vec![Principal {
            name,
            token_sha256: hex(&Sha256::digest(token.as_bytes())),
            workspaces,
            nio_skills,
            nio_plugins,
        }],
    };
    write_new(path, &file)?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combined_bearer_tokens() {
        let mut rand_bytes = [0u8; 8];
        OsRng.fill_bytes(&mut rand_bytes);
        let temp = std::env::temp_dir().join(format!("niodb_auth_test_{}", hex(&rand_bytes)));
        let _ = std::fs::create_dir_all(&temp);
        let auth_path = temp.join("auth.json");
        let pair = init_pair_with_capabilities(
            &auth_path,
            "test-app".into(),
            vec!["default".into()],
            vec!["skill-a".into()],
            vec!["plugin-b".into()],
        )
        .unwrap();

        let principals = load(&auth_path).unwrap();

        // 1. Single client token
        let p_client = authenticate(&principals, &pair.client_token);
        assert!(p_client.is_some());
        assert_eq!(p_client.unwrap().name, "test-app");

        // 2. Single secret token
        let p_secret = authenticate(&principals, &pair.secret_token);
        assert!(p_secret.is_some());
        assert_eq!(p_secret.unwrap().name, "test-app-secret");

        // 3. Combined token (colon separator)
        let combined_colon = format!("{}:{}", pair.client_token, pair.secret_token);
        let p_comb1 = authenticate(&principals, &combined_colon);
        assert!(p_comb1.is_some());
        let c1 = p_comb1.unwrap();
        assert_eq!(c1.name, "test-app");
        assert!(c1.nio_skills.contains(&"skill-a".to_string()));

        // 4. Combined token (dot separator)
        let combined_dot = format!("{}.{}", pair.client_token, pair.secret_token);
        let p_comb2 = authenticate(&principals, &combined_dot);
        assert!(p_comb2.is_some());

        // 5. Invalid combinations
        let invalid_comb = format!("{}:invalid_secret", pair.client_token);
        assert!(authenticate(&principals, &invalid_comb).is_none());

        let invalid_single = "niodb_invalid_token";
        assert!(authenticate(&principals, invalid_single).is_none());
    }
}
