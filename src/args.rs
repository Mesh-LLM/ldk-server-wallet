//! The `args` of a `ldk-server-wallet` `[[plugin]]` stanza.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "ldk-server-wallet", no_binary_name = true)]
pub struct PluginArgs {
    /// LDK Server gRPC address as host:port, without a scheme.
    #[arg(long, value_name = "HOST:PORT")]
    pub server: String,
    /// Absolute path to the server's TLS certificate (`<storage_dir>/tls.crt`).
    #[arg(long, value_name = "PATH")]
    pub cert_file: PathBuf,
    /// Absolute path to a file holding the hex macaroon. It carries the node's
    /// spending authority, so it is never taken inline; keep the file at mode
    /// 0600.
    #[arg(long, value_name = "PATH")]
    pub macaroon_file: PathBuf,
    /// Seconds to wait for a server response.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    pub request_timeout_secs: Option<u64>,
    /// Seconds to wait for a payment request before treating it as uncertain.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    pub pay_timeout_secs: Option<u64>,
    /// Seconds between payment lookups while the event stream is connected.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    pub backstop_poll_secs: Option<u64>,
}

/// Validated connection details.
pub struct Connection {
    pub server: String,
    pub cert_pem: Vec<u8>,
    pub macaroon: String,
}

#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Any single request other than a payment.
    pub request_timeout: Duration,
    /// A `bolt11_send` request. Past this the outcome is uncertain.
    pub pay_timeout: Duration,
    /// Lookups while the event stream is connected, covering events the
    /// server drops for a lagging subscriber.
    pub backstop_poll: Duration,
    /// Lookups while the event stream is down.
    pub disconnected_poll: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            pay_timeout: Duration::from_secs(120),
            backstop_poll: Duration::from_secs(30),
            disconnected_poll: Duration::from_secs(2),
        }
    }
}

impl PluginArgs {
    /// Parse the arguments after the executable name.
    pub fn parse_from_args(args: impl IntoIterator<Item = String>) -> Result<Self> {
        Self::try_parse_from(args)
            .map_err(|error| anyhow::anyhow!("invalid ldk-server-wallet args: {error}"))
    }

    pub fn timing(&self) -> Timing {
        let defaults = Timing::default();
        Timing {
            request_timeout: self
                .request_timeout_secs
                .map_or(defaults.request_timeout, Duration::from_secs),
            pay_timeout: self
                .pay_timeout_secs
                .map_or(defaults.pay_timeout, Duration::from_secs),
            backstop_poll: self
                .backstop_poll_secs
                .map_or(defaults.backstop_poll, Duration::from_secs),
            disconnected_poll: defaults.disconnected_poll,
        }
    }

    /// Read the certificate and macaroon. Errors never include file contents.
    pub fn connection(&self) -> Result<Connection> {
        let server = self.server.trim();
        if server.is_empty() {
            bail!("ldk-server-wallet `--server` must not be empty");
        }
        if server.contains("://") {
            bail!("ldk-server-wallet `--server` is host:port without a scheme");
        }
        let cert_file = absolute(&self.cert_file, "--cert-file")?;
        let macaroon_file = absolute(&self.macaroon_file, "--macaroon-file")?;
        let cert_pem = std::fs::read(cert_file)
            .with_context(|| format!("read LDK Server certificate {}", cert_file.display()))?;
        let macaroon = std::fs::read_to_string(macaroon_file)
            .with_context(|| format!("read LDK Server macaroon {}", macaroon_file.display()))?
            .trim()
            .to_owned();
        if macaroon.is_empty() {
            bail!("{} is empty", macaroon_file.display());
        }
        Ok(Connection {
            server: server.to_owned(),
            cert_pem,
            macaroon,
        })
    }
}

fn absolute<'a>(path: &'a Path, flag: &str) -> Result<&'a Path> {
    if !path.is_absolute() {
        bail!("ldk-server-wallet `{flag}` must be an absolute path");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<PluginArgs> {
        PluginArgs::parse_from_args(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn required_flags_are_named_and_unknown_flags_are_refused() {
        let missing = parse(&[]).unwrap_err().to_string();
        assert!(missing.contains("--server"), "{missing}");
        assert!(missing.contains("--macaroon-file"), "{missing}");
        let every_flag = [
            "--server",
            "127.0.0.1:3536",
            "--cert-file",
            "/x/tls.crt",
            "--macaroon-file",
            "/x/macaroon",
            "--request-timeout-secs",
            "1",
            "--pay-timeout-secs",
            "1",
            "--backstop-poll-secs",
            "1",
        ];
        let timing = parse(&every_flag).unwrap().timing();
        assert_eq!(timing.backstop_poll, Duration::from_secs(1));
        let mut inline = every_flag[..6].to_vec();
        inline.extend(["--macaroon", "0201"]);
        assert!(
            parse(&inline).is_err(),
            "an inline macaroon must be refused"
        );
        let mut zero = every_flag.to_vec();
        zero[7] = "0";
        assert!(parse(&zero).is_err());
    }

    #[test]
    fn connection_reads_files_and_rejects_bad_values() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("tls.crt");
        let macaroon = dir.path().join("macaroon");
        std::fs::write(&cert, b"-----BEGIN CERTIFICATE-----").unwrap();
        std::fs::write(&macaroon, "0201abcd\n").unwrap();
        let plugin_args = |server: &str, cert_file: &Path| PluginArgs {
            server: server.into(),
            cert_file: cert_file.into(),
            macaroon_file: macaroon.clone(),
            request_timeout_secs: None,
            pay_timeout_secs: None,
            backstop_poll_secs: None,
        };

        let connection = plugin_args("127.0.0.1:3536", &cert).connection().unwrap();
        assert_eq!(connection.server, "127.0.0.1:3536");
        assert_eq!(connection.macaroon, "0201abcd");

        assert!(
            plugin_args("https://127.0.0.1:3536", &cert)
                .connection()
                .is_err()
        );
        assert!(
            plugin_args("127.0.0.1:3536", Path::new("tls.crt"))
                .connection()
                .is_err()
        );
        assert!(plugin_args(" ", &cert).connection().is_err());
    }
}
