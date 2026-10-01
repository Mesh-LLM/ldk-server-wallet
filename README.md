# LDK Server wallet for mesh-llm

A [mesh-llm](https://github.com/Mesh-LLM/mesh-llm) wallet backed by an
[LDK Server](https://github.com/lightningdevkit/ldk-server) node. It is an
external plugin serving the `wallet.v1` capability, with a reusable
`mesh-wallet-ldk-server` library. Mesh keeps the ledger, budgets and recovery.

`ldk-server-client` is not published to crates.io (it is pinned by git
revision) and brings its own HTTP/TLS stack, which is why this wallet lives
outside the mesh-llm host.

## Install

No release is published yet. Once one is:

```sh
mesh-llm plugins install benthecarman/ldk-server-wallet
```

Until then, build an archive (see Development) and install it with:

```sh
mesh-llm plugins install --name ldk-server-wallet --version 0.1.0 --archive dist/ldk-server-wallet-v0.1.0-<target>.tar.gz
```

Use a mesh-llm build with payments enabled. If another wallet plugin is also
installed, choose this one in `config.toml` with `[payments] wallet =
"ldk-server-wallet"`.

## Configure

The plugin takes its configuration as the `args` of its `[[plugin]]` stanza in
mesh-llm's `config.toml`; see `config.toml` here for a complete example:

```toml
[[plugin]]
name = "ldk-server-wallet"
args = [
  "--server", "127.0.0.1:3536",
  "--cert-file", "/home/me/.ldk-server/tls.crt",
  "--macaroon-file", "/home/me/.mesh-llm/ldk-server.macaroon",
]
```

| Argument | Default | Meaning |
|---|---|---|
| `--server <HOST:PORT>` | required | The server's gRPC address, without a scheme |
| `--cert-file <PATH>` | required | Absolute path to the server's TLS certificate (`<ldk-server storage dir>/tls.crt`) |
| `--macaroon-file <PATH>` | required | Absolute path to a file holding a hex macaroon |
| `--request-timeout-secs <SECS>` | 30 | How long to wait for a server response |
| `--pay-timeout-secs <SECS>` | 120 | How long to wait for a payment request before treating it as uncertain |
| `--backstop-poll-secs <SECS>` | 30 | Time between payment lookups while the event stream is connected |

Mint a macaroon limited to what the wallet uses: node info, channel listing,
BOLT11 receive and send, payment reads, and event reads. Keep the file at mode
0600; the macaroon authorizes spending, so it is never accepted inline. A
change takes effect when mesh-llm restarts.

Macaroons are bound to the request time; the mesh-llm and LDK Server hosts'
clocks must agree within a minute.

## What it guarantees

- **Fees.** Every payment passes the host's fee headroom to LDK as
  `max_total_routing_fee_msat`, which LDK enforces across all retries; a route
  that would exceed it is refused before anything is sent.
- **Idempotency.** An outgoing payment is looked up by hash before sending, and
  a duplicate refusal is resolved by looking the payment up again.
- **Not submitted vs uncertain.** Invalid-request, authorization and LDK
  refusals (no route, insufficient funds) happen before any HTLC is sent and
  are reported as not submitted. Server errors, timeouts and transport failures
  are uncertain and reconciled by payment hash.
- **Arrival.** The node records an incoming payment as pending once its HTLCs
  are committed and it is claiming them, which is reported as `claiming`.
- **Network.** Neither the plugin nor LDK Server checks an invoice's network.
  An invoice for another network is routed to its payee key like any other,
  which normally fails for want of a route.

LDK Server keeps no record of an invoice until money arrives, and then keys
the payment by an HTLC-derived id. The plugin keeps
`issued-invoices.json` in its wallet directory
(`<config dir>/payments/wallets/ldk-server-wallet/`), written durably before an
invoice is returned, so it can report an unpaid invoice as pending (and failed
a minute after it expires) and find the incoming payment by hash.

Settlement waits are driven by the node's event stream. Those events are best
effort (dropped while disconnected, skipped when a subscriber lags), so an
event only triggers a lookup; a lookup every 30 seconds covers missed events,
every reconnect re-checks all waits, and waits poll every 2 seconds while the
stream is down.

## Development

Use `just check`, `just test`, `just build` and `just package`. The mesh-llm
SDK crates (`mesh-llm-plugin`, `mesh-llm-wallet`) are pinned by git revision in
`Cargo.toml` until they are published. `just package` packages the native host
platform only; the release workflow builds macOS, Linux and Windows archives.
See [RELEASING.md](RELEASING.md).

Tests never contact a real node or move money: the provider is tested against a
scripted node, and the installed-process test installs the real executable
with mesh's installer and checks the plugin handshake without opening a
wallet.

## Status

It has not yet been run against a live LDK Server on regtest. `ldk-server`
itself warns that its API is not yet stable; the pinned revision is recorded in
`Cargo.toml`.
