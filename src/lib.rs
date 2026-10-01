//! LDK Server as an external mesh-llm wallet plugin.
//!
//! It is maintained outside mesh-llm: it depends only on the
//! `mesh-llm-wallet` and `mesh-llm-plugin` SDK crates, builds as the
//! `ldk-server-wallet` executable, and installs with `mesh-llm plugins
//! install`. The host finds it through the `wallet.v1` capability like any
//! other wallet. It takes its configuration as the `args` of its `[[plugin]]`
//! stanza; see [`args::PluginArgs`].

mod api;
mod args;
mod events;
mod index;
mod provider;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use ldk_server_client::client::LdkServerClient;
use mesh_llm_plugin::PluginRuntime;
use mesh_llm_wallet::backend::{OpenedWallet, WalletBackend};
use mesh_llm_wallet::contract::{WalletFeatures, WalletIdentity};
use mesh_llm_wallet::plugin_server::wallet_plugin;

use crate::api::{NodeApi, ServerApi};
use crate::args::PluginArgs;
use crate::index::InvoiceIndex;
use crate::provider::LdkProvider;

/// Plugin name the host installs this implementation under.
pub const PLUGIN_NAME: &str = "ldk-server-wallet";
const VERSION: &str = env!("CARGO_PKG_VERSION");

struct LdkServerBackend {
    args: PluginArgs,
}

fn features() -> WalletFeatures {
    WalletFeatures {
        amountless_invoices: true,
    }
}

#[async_trait]
impl WalletBackend for LdkServerBackend {
    fn provider_name(&self) -> &'static str {
        "ldk-server"
    }

    async fn open(&self, directory: &Path) -> Result<OpenedWallet> {
        let timing = self.args.timing();
        let connection = self.args.connection()?;
        let client =
            LdkServerClient::new(connection.server, connection.macaroon, &connection.cert_pem)
                .map_err(|error| anyhow::anyhow!("LDK Server client: {error}"))?;
        let api: Arc<dyn NodeApi> = Arc::new(ServerApi::new(
            client,
            timing.request_timeout,
            timing.pay_timeout,
        ));
        let info = api.node_info().await.context("LDK Server get_node_info")?;
        let index = Arc::new(InvoiceIndex::open(directory)?);
        Ok(OpenedWallet {
            identity: WalletIdentity {
                wallet_id: info.node_id,
                provider: "ldk-server".into(),
                network: info.network.into(),
            },
            provider: Arc::new(LdkProvider::new(api, index, timing)),
            created: false,
            features: features(),
        })
    }
}

/// Serve the `wallet.v1` capability on the host-supplied plugin endpoint until
/// the host closes the connection. `name` is the plugin name the host launched
/// this process under and `args` its `[[plugin]]` stanza's `args`.
pub async fn run_plugin(name: String, args: Vec<String>) -> Result<()> {
    let backend = LdkServerBackend {
        args: PluginArgs::parse_from_args(args)?,
    };
    PluginRuntime::run(wallet_plugin(name, VERSION, backend)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_llm_plugin::Plugin;

    #[test]
    fn manifest_declares_the_wallet_capability() {
        let args = PluginArgs::parse_from_args(
            [
                "--server",
                "127.0.0.1:3536",
                "--cert-file",
                "/x",
                "--macaroon-file",
                "/y",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        let plugin = wallet_plugin(PLUGIN_NAME, VERSION, LdkServerBackend { args });
        let manifest = plugin.manifest().unwrap();
        assert_eq!(manifest.capabilities, ["wallet.v1"]);
    }

    #[test]
    fn features_claim_what_ldk_supports() {
        let features = features();
        assert!(features.amountless_invoices);
    }
}
