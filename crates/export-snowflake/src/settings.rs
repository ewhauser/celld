//! The loader's settings from the environment, shared by
//! `celld-export-loader` and `celld export` when it audits Snowflake. The
//! README's "The loader" lists them.
#![allow(clippy::disallowed_methods)] // A host tool: the host's clock, key file and process id.

use std::time::Duration;

use crate::consume::Limits;
use crate::sql_api::{Clock, Connection, KeyPair, SqlApi};
use crate::streaming::Streaming;
use crate::{Deployment, Loader, LoaderConfig};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

fn env(name: &str) -> Result<String, Error> {
    std::env::var(name).map_err(|_| format!("{name} is not set").into())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// `name` as a number, or `default` when it is not set.
pub fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, Error> {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .map_err(|_| format!("{name} must be a number, not {v:?}").into()),
        Err(_) => Ok(default),
    }
}

/// Where and as whom: the `SNOWFLAKE_*` settings and the key they name.
pub fn credentials() -> Result<(Connection, KeyPair, Clock), Error> {
    let pem = std::fs::read_to_string(env("SNOWFLAKE_PRIVATE_KEY_FILE")?)?;
    let passphrase = std::env::var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").ok();
    let key = KeyPair::from_pem(&pem, passphrase.as_deref())?;
    let connection = Connection {
        account: env("SNOWFLAKE_ACCOUNT")?,
        user: env("SNOWFLAKE_USER")?,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        database: env("SNOWFLAKE_DATABASE")?,
        schema: env("SNOWFLAKE_SCHEMA")?,
        warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        url: std::env::var("SNOWFLAKE_URL").ok(),
        statement_timeout: 600,
    };
    let clock: Clock = Box::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    });
    Ok((connection, key, clock))
}

/// Where batches land: Snowpipe Streaming, as the same user.
pub fn streaming() -> Result<Streaming, Error> {
    let (connection, key, clock) = credentials()?;
    Ok(Streaming::new(&connection, key, clock))
}

/// When a batch lands: `EXPORT_BATCH_RECORDS` and `EXPORT_BATCH_BYTES`.
pub fn limits() -> Result<Limits, Error> {
    let d = Limits::default();
    Ok(Limits {
        records: number("EXPORT_BATCH_RECORDS", d.records)?.max(1),
        bytes: number("EXPORT_BATCH_BYTES", d.bytes)?.max(1),
    })
}

/// How long landed rows may take to become queryable:
/// `EXPORT_VISIBLE_SECONDS`, default 300.
pub fn visible_timeout() -> Result<Duration, Error> {
    Ok(Duration::from_secs(number("EXPORT_VISIBLE_SECONDS", 300)?))
}

pub fn loader() -> Result<Loader<SqlApi>, Error> {
    let config = LoaderConfig {
        deployment: Deployment {
            warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        },
        target_lag: env_or("EXPORT_TARGET_LAG", "1 minute"),
        dynamic_table_prefix: env_or("EXPORT_DYNAMIC_TABLE_PREFIX", "CF"),
    };
    let (connection, key, clock) = credentials()?;
    Ok(Loader::new(SqlApi::new(connection, key, clock), config))
}

/// A source suffix unique to one run of `what`, for [`crate::consume::Batch::tagged`]
/// and [`Loader::settle`]. It holds no `%` or `_`, since `visible` matches
/// it with LIKE.
pub fn run_tag(what: &str) -> String {
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!(" ({what} {started}-{})", std::process::id())
}

/// Wait with backoff from 100ms to 2s until `timeout` has passed since the
/// first call; then say to stop. For [`Loader::settle`].
pub fn backoff(timeout: Duration) -> impl FnMut() -> bool {
    let deadline = std::time::Instant::now() + timeout;
    let mut pause = Duration::from_millis(100);
    move || {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_secs(2));
        true
    }
}
