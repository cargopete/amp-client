use std::{env, path::PathBuf};

use crate::error::{Error, Result};

/// Resolves an auth token using the standard Amp priority order:
/// 1. Explicit token passed to the builder
/// 2. `AMP_AUTH_TOKEN` environment variable
/// 3. `~/.amp/cache/amp_cli_auth` file (written by `ampctl login`)
pub(crate) fn resolve(explicit: Option<&str>) -> Result<Option<String>> {
    if let Some(t) = explicit {
        return Ok(Some(t.to_owned()));
    }

    if let Ok(t) = env::var("AMP_AUTH_TOKEN") {
        if !t.is_empty() {
            return Ok(Some(t));
        }
    }

    let path = cache_file();
    if path.exists() {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| Error::Auth(format!("reading {}: {}", path.display(), e)))?;
        let token = content.trim().to_owned();
        if !token.is_empty() {
            return Ok(Some(token));
        }
    }

    Ok(None)
}

fn cache_file() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_owned());
    PathBuf::from(home).join(".amp/cache/amp_cli_auth")
}
