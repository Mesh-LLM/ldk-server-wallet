Standalone LDK Server wallet provider for mesh-llm's `wallet.v1` capability.
Install with:

```sh
mesh-llm plugins install benthecarman/ldk-server-wallet
```

Then pass the server address, TLS certificate and macaroon file in the
plugin's `[[plugin]]` stanza (see the README). Use a mesh-llm host with
payments enabled. Installation does not authorize spending; mesh-llm keeps
policy, budgets and the ledger.

Every payment passes the host's fee headroom to LDK as a routing fee limit,
which LDK enforces across retries. The plugin keeps an index of the invoices
it issued in its wallet directory, because LDK Server records an invoice only
once it is paid.

Native archives and SHA-256 sidecars cover macOS Apple Silicon/Intel, Linux
ARM64/x86_64 and Windows ARM64/x86_64. Artifacts are not notarized or
Authenticode-signed. Not yet validated against a live LDK Server.
