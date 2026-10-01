//! Standalone launcher; the node is contacted only through wallet.v1 RPC.
use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let name = std::env::var("MESH_LLM_PLUGIN_NAME")
        .context("Launch this executable through mesh-llm plugins, not directly")?;
    mesh_wallet_ldk_server::run_plugin(name, std::env::args().skip(1).collect()).await
}
