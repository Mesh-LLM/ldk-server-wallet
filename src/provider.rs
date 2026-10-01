//! `WalletProvider` over LDK Server.
//!
//! LDK enforces the host's fee cap itself (`max_total_routing_fee_msat` holds
//! across every retry), reports an arriving payment as a pending inbound
//! record while it claims it, and names outgoing payments by their payment
//! hash. What it lacks, this provider supplies: a record of issued invoices
//! (the node knows nothing of an invoice until money arrives), creation times,
//! and a way to find an incoming payment by hash.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use mesh_llm_wallet::invoice::Invoice;
use mesh_llm_wallet::provider::{Balance, PayError, PaymentStatus, Transaction, WalletProvider};
use tokio::sync::broadcast;

use crate::api::{ApiError, ApiErrorKind, NodeApi, NodePayment};
use crate::args::Timing;
use crate::events::{EventFeed, Wake};
use crate::index::{InvoiceIndex, IssuedInvoice};

const INVOICE_DESCRIPTION: &str = "mesh-llm";
/// BOLT11 timestamps have one-second resolution.
const EXPIRY_TOLERANCE_MS: u64 = 2_000;
/// An unpaid issued invoice is reported failed only this long after expiry,
/// so a payment that arrived just in time has been recorded by the node.
const EXPIRY_GRACE_MS: u64 = 60_000;
/// `transactions` never pages through more than this many payments.
const MAX_LIST_PAGES: usize = 20;

pub struct LdkProvider {
    api: Arc<dyn NodeApi>,
    index: Arc<InvoiceIndex>,
    feed: EventFeed,
    timing: Timing,
}

impl LdkProvider {
    pub fn new(api: Arc<dyn NodeApi>, index: Arc<InvoiceIndex>, timing: Timing) -> Self {
        let feed = EventFeed::spawn(Arc::clone(&api), Arc::clone(&index));
        Self {
            api,
            index,
            feed,
            timing,
        }
    }

    fn transaction(&self, payment: NodePayment) -> Transaction {
        let issued = payment
            .payment_hash
            .as_deref()
            .and_then(|hash| self.index.get(hash));
        let updated_at_ms = payment.updated_at_secs.saturating_mul(1000);
        let terminal = payment.status != PaymentStatus::Pending;
        Transaction {
            id: payment.payment_id,
            payment_hash: payment.payment_hash,
            inbound: payment.inbound,
            amount_msat: payment.amount_msat.unwrap_or(0),
            fee_msat: payment.fee_paid_msat.unwrap_or(0),
            status: payment.status,
            // The node only records an incoming payment once its HTLCs are
            // irrevocably committed, and then claims it automatically.
            claiming: payment.inbound && payment.status == PaymentStatus::Pending,
            status_msg: None,
            // The node does not keep creation times; the index does for what
            // this wallet issued.
            created_at_ms: issued.map_or(updated_at_ms, |issued| issued.created_at_ms),
            settled_at_ms: terminal.then_some(updated_at_ms),
        }
    }

    /// An issued invoice the node has no payment for yet.
    fn unpaid(payment_hash: &str, issued: &IssuedInvoice, now_ms: u64) -> Transaction {
        let expired = issued.expires_at_ms.saturating_add(EXPIRY_GRACE_MS) < now_ms;
        Transaction {
            id: payment_hash.to_owned(),
            payment_hash: Some(payment_hash.to_owned()),
            inbound: true,
            amount_msat: issued.amount_msat.unwrap_or(0),
            fee_msat: 0,
            status: if expired {
                PaymentStatus::Failed
            } else {
                PaymentStatus::Pending
            },
            claiming: false,
            status_msg: Some(if expired { "expired" } else { "unpaid" }.into()),
            created_at_ms: issued.created_at_ms,
            settled_at_ms: None,
        }
    }

    /// Find the node's record of an incoming payment for an issued invoice.
    async fn find_incoming(
        &self,
        payment_hash: &str,
        issued: &IssuedInvoice,
    ) -> Result<Option<NodePayment>, ApiError> {
        if let Some(id) = &issued.payment_id
            && let Some(payment) = self.api.payment(id).await?
        {
            return Ok(Some(payment));
        }
        // Payments are listed newest first by creation, and an update never
        // precedes creation, so once records were last updated before the
        // invoice existed, nothing further down can be its payment.
        let issued_at_secs = issued.created_at_ms / 1000;
        let mut page_token = None;
        loop {
            let (payments, next) = self.api.payments_page(page_token).await?;
            for payment in payments {
                if payment.inbound && payment.payment_hash.as_deref() == Some(payment_hash) {
                    self.index
                        .record_payment_id(payment_hash, &payment.payment_id);
                    return Ok(Some(payment));
                }
                if payment.updated_at_secs.saturating_add(1) < issued_at_secs {
                    return Ok(None);
                }
            }
            match next {
                Some(token) => page_token = Some(token),
                None => return Ok(None),
            }
        }
    }

    async fn lookup_node(&self, payment_hash: &str) -> Result<Option<Transaction>, ApiError> {
        // Outgoing BOLT11 payments are keyed by their hash.
        if let Some(payment) = self.api.payment(payment_hash).await? {
            return Ok(Some(self.transaction(payment)));
        }
        let Some(issued) = self.index.get(payment_hash) else {
            return Ok(None);
        };
        Ok(Some(
            match self.find_incoming(payment_hash, &issued).await? {
                Some(payment) => self.transaction(payment),
                None => Self::unpaid(payment_hash, &issued, mesh_llm_wallet::now_ms()),
            },
        ))
    }

    /// Wait until `settled` holds, looking the payment up whenever an event
    /// might concern it and otherwise on a poll. Transport failures while
    /// waiting are retried; the caller owns the deadline.
    async fn wait_until(
        &self,
        payment_hash: &str,
        settled: impl Fn(&Transaction) -> bool,
    ) -> Result<Transaction> {
        // Subscribe before the first lookup so nothing between them is lost.
        let mut wake = self.feed.subscribe();
        loop {
            match self.lookup_node(payment_hash).await {
                Ok(Some(payment)) if settled(&payment) => return Ok(payment),
                Ok(_) => {}
                Err(error) if error.kind == ApiErrorKind::Uncertain => {
                    tracing::debug!(target: "wallet_ldk_server", %error, "lookup failed while waiting");
                }
                Err(error) => return Err(error.into()),
            }
            let poll = if self.feed.is_connected() {
                self.timing.backstop_poll
            } else {
                self.timing.disconnected_poll
            };
            tokio::select! {
                () = woken(&mut wake, payment_hash) => {}
                () = tokio::time::sleep(poll) => {}
            }
        }
    }
}

/// Resolve on a wake-up that may concern `payment_hash`. Lagging behind the
/// feed resolves too: a missed wake-up may have been this payment's.
async fn woken(wake: &mut broadcast::Receiver<Wake>, payment_hash: &str) {
    loop {
        match wake.recv().await {
            Ok(Wake::Resync) => return,
            Ok(Wake::Payment(hash)) if hash == payment_hash => return,
            Ok(Wake::Payment(_)) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => return,
            Err(broadcast::error::RecvError::Closed) => std::future::pending().await,
        }
    }
}

#[async_trait]
impl WalletProvider for LdkProvider {
    async fn balance(&self) -> Result<Balance> {
        Ok(Balance {
            spendable_msat: self.api.sendable_msat().await?,
        })
    }

    async fn transactions(&self, limit: usize) -> Result<Vec<Transaction>> {
        anyhow::ensure!(
            (1..=1000).contains(&limit),
            "transaction limit must be between 1 and 1000"
        );
        let mut collected = Vec::new();
        let mut page_token = None;
        for _ in 0..MAX_LIST_PAGES {
            let (payments, next) = self.api.payments_page(page_token).await?;
            collected.extend(payments.into_iter().map(|p| self.transaction(p)));
            match next {
                Some(token) if collected.len() < limit => page_token = Some(token),
                _ => break,
            }
        }
        collected.truncate(limit);
        Ok(collected)
    }

    async fn create_invoice(&self, amount_msat: Option<u64>, expiry_secs: u32) -> Result<Invoice> {
        anyhow::ensure!(
            amount_msat != Some(0),
            "zero amount invoice is not supported"
        );
        anyhow::ensure!(expiry_secs > 0, "invoice expiry must be positive");
        let bolt11 = self
            .api
            .create_invoice(amount_msat, INVOICE_DESCRIPTION, expiry_secs)
            .await?;
        let invoice = Invoice::parse(&bolt11)?;
        let now_ms = mesh_llm_wallet::now_ms();
        let latest = now_ms
            .saturating_add(u64::from(expiry_secs) * 1000)
            .saturating_add(EXPIRY_TOLERANCE_MS);
        if invoice.expires_at_ms > latest {
            bail!("node ignored the requested invoice expiry of {expiry_secs}s");
        }
        if invoice.amount_msat != amount_msat {
            bail!("node created an invoice for a different amount");
        }
        self.index
            .insert(
                &invoice.payment_hash,
                IssuedInvoice {
                    created_at_ms: now_ms,
                    expires_at_ms: invoice.expires_at_ms,
                    amount_msat,
                    payment_id: None,
                },
                now_ms,
            )
            .context("record issued invoice")?;
        Ok(invoice)
    }

    async fn pay(
        &self,
        invoice: &Invoice,
        amount_msat: u64,
        max_total_msat: u64,
    ) -> Result<Transaction, PayError> {
        invoice
            .validate_payment(amount_msat, mesh_llm_wallet::now_ms())
            .map_err(PayError::NotSubmitted)?;
        let max_fee_msat = max_total_msat.checked_sub(amount_msat).ok_or_else(|| {
            PayError::NotSubmitted(anyhow!("payment amount exceeds the authorized total"))
        })?;
        if self.index.get(&invoice.payment_hash).is_some() {
            return Err(PayError::NotSubmitted(anyhow!(
                "cannot pay this wallet's own invoice"
            )));
        }
        // The payment hash is the idempotency key: report an existing payment
        // instead of paying again.
        match self.api.payment(&invoice.payment_hash).await {
            Ok(Some(existing)) if existing.inbound => {
                return Err(PayError::NotSubmitted(anyhow!(
                    "cannot pay this wallet's own invoice"
                )));
            }
            Ok(Some(existing)) => return Ok(self.transaction(existing)),
            Ok(None) => {}
            Err(error) => {
                return Err(PayError::NotSubmitted(anyhow!(
                    "payment lookup failed before submission: {error}"
                )));
            }
        }
        let amount = invoice.amount_msat.is_none().then_some(amount_msat);
        match self.api.send(&invoice.bolt11, amount, max_fee_msat).await {
            Ok(payment_id) => Ok(match self.api.payment(&payment_id).await {
                Ok(Some(payment)) => self.transaction(payment),
                // Submitted; the record is not readable yet.
                _ => Transaction {
                    id: payment_id,
                    payment_hash: Some(invoice.payment_hash.clone()),
                    inbound: false,
                    amount_msat,
                    fee_msat: 0,
                    status: PaymentStatus::Pending,
                    claiming: false,
                    status_msg: None,
                    created_at_ms: mesh_llm_wallet::now_ms(),
                    settled_at_ms: None,
                },
            }),
            Err(error) => Err(self.classify_send_failure(invoice, error).await),
        }
    }

    async fn lookup(&self, payment_hash: &str) -> Result<Option<Transaction>> {
        Ok(self.lookup_node(payment_hash).await?)
    }

    async fn wait_for_payment(&self, payment_hash: &str) -> Result<Transaction> {
        self.wait_until(payment_hash, |payment| {
            payment.status != PaymentStatus::Pending
        })
        .await
    }

    async fn wait_for_arrival(&self, payment_hash: &str) -> Result<Transaction> {
        self.wait_until(payment_hash, |payment| {
            payment.status != PaymentStatus::Pending || payment.is_claiming()
        })
        .await
    }
}

impl LdkProvider {
    /// Decide whether a failed `bolt11_send` could have moved money.
    ///
    /// LDK refuses a payment it cannot start (no route, duplicate, no funds)
    /// before sending any HTLC, so those are not submitted, unless the refusal
    /// was a duplicate of a payment that is still in flight or succeeded, in
    /// which case that payment is the answer. Server and transport failures
    /// stay uncertain.
    async fn classify_send_failure(&self, invoice: &Invoice, error: ApiError) -> PayError {
        match error.kind {
            ApiErrorKind::InvalidRequest | ApiErrorKind::Auth => {
                PayError::NotSubmitted(anyhow!("node refused payment: {error}"))
            }
            ApiErrorKind::Lightning => match self.api.payment(&invoice.payment_hash).await {
                Ok(Some(existing))
                    if !existing.inbound && existing.status != PaymentStatus::Failed =>
                {
                    PayError::Uncertain(anyhow!(
                        "a payment for this invoice is already {:?}; reconcile by payment hash",
                        existing.status
                    ))
                }
                Ok(_) => PayError::NotSubmitted(anyhow!("node refused payment: {error}")),
                Err(lookup) => PayError::Uncertain(anyhow!(
                    "node refused payment ({error}) and the follow-up lookup failed: {lookup}"
                )),
            },
            ApiErrorKind::Uncertain => PayError::Uncertain(anyhow!(
                "payment outcome uncertain; reconcile by payment hash: {error}"
            )),
        }
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
