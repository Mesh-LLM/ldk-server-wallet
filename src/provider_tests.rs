//! The provider against a scripted node, so every money-moving rule is
//! checked without LDK Server.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use bitcoin::secp256k1::{Secp256k1, SecretKey};
use lightning_invoice::{Currency, InvoiceBuilder, PaymentHash, PaymentSecret};
use tokio::sync::mpsc;

use super::*;
use crate::api::{ApiResult, NodeInfo, PaymentEvents};

/// One `list_payments` page and the token for the next.
type Page = (Vec<NodePayment>, Option<String>);

#[derive(Default)]
struct FakeNode {
    payments: Mutex<HashMap<String, NodePayment>>,
    /// Pages of `list_payments`, keyed by the token that fetches them.
    pages: Mutex<HashMap<Option<String>, Page>>,
    invoices: Mutex<VecDeque<String>>,
    send_results: Mutex<VecDeque<ApiResult<String>>>,
    lookup_failures: Mutex<VecDeque<ApiError>>,
    calls: Mutex<Vec<String>>,
    events: Mutex<Option<mpsc::UnboundedSender<NodePayment>>>,
    sent: Mutex<Vec<(Option<u64>, u64)>>,
    /// A payment the node records while refusing a send, as a concurrent
    /// duplicate would.
    recorded_on_send: Mutex<Option<NodePayment>>,
}

impl FakeNode {
    fn call(&self, name: String) {
        self.calls.lock().unwrap().push(name);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn store(&self, payment: NodePayment) {
        self.payments
            .lock()
            .unwrap()
            .insert(payment.payment_id.clone(), payment);
    }

    fn emit(&self, payment: NodePayment) {
        if let Some(events) = self.events.lock().unwrap().as_ref() {
            let _ = events.send(payment);
        }
    }
}

struct FakeEvents(mpsc::UnboundedReceiver<NodePayment>);

#[async_trait]
impl PaymentEvents for FakeEvents {
    async fn next(&mut self) -> Option<ApiResult<Option<NodePayment>>> {
        self.0.recv().await.map(|payment| Ok(Some(payment)))
    }
}

#[async_trait]
impl NodeApi for FakeNode {
    async fn node_info(&self) -> ApiResult<NodeInfo> {
        unimplemented!("not used by the provider")
    }

    async fn sendable_msat(&self) -> ApiResult<u64> {
        Ok(42_000)
    }

    async fn create_invoice(&self, _: Option<u64>, _: &str, _: u32) -> ApiResult<String> {
        self.call("create_invoice".into());
        Ok(self.invoices.lock().unwrap().pop_front().unwrap())
    }

    async fn send(
        &self,
        _: &str,
        amount_msat: Option<u64>,
        max_fee_msat: u64,
    ) -> ApiResult<String> {
        self.call("send".into());
        self.sent.lock().unwrap().push((amount_msat, max_fee_msat));
        if let Some(payment) = self.recorded_on_send.lock().unwrap().take() {
            self.store(payment);
        }
        self.send_results.lock().unwrap().pop_front().unwrap()
    }

    async fn payment(&self, payment_id: &str) -> ApiResult<Option<NodePayment>> {
        self.call(format!("payment:{payment_id}"));
        if let Some(error) = self.lookup_failures.lock().unwrap().pop_front() {
            return Err(error);
        }
        Ok(self.payments.lock().unwrap().get(payment_id).cloned())
    }

    async fn payments_page(
        &self,
        page_token: Option<String>,
    ) -> ApiResult<(Vec<NodePayment>, Option<String>)> {
        self.call(format!("page:{page_token:?}"));
        Ok(self
            .pages
            .lock()
            .unwrap()
            .get(&page_token)
            .cloned()
            .unwrap_or_default())
    }

    async fn subscribe(&self) -> ApiResult<Box<dyn PaymentEvents>> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.events.lock().unwrap() = Some(tx);
        Ok(Box::new(FakeEvents(rx)))
    }
}

struct Harness {
    node: Arc<FakeNode>,
    provider: LdkProvider,
    _dir: tempfile::TempDir,
}

fn harness() -> Harness {
    let node = Arc::new(FakeNode::default());
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(InvoiceIndex::open(dir.path()).unwrap());
    let provider = LdkProvider::new(
        Arc::clone(&node) as Arc<dyn NodeApi>,
        index,
        Timing {
            // Long polls: a test only finishes quickly if an event wakes it.
            backstop_poll: Duration::from_secs(60),
            disconnected_poll: Duration::from_secs(60),
            ..Timing::default()
        },
    );
    Harness {
        node,
        provider,
        _dir: dir,
    }
}

fn signed_invoice(hash_byte: u8, amount_msat: Option<u64>, expiry_secs: u64) -> String {
    let secret = SecretKey::from_slice(&[7; 32]).unwrap();
    let mut builder = InvoiceBuilder::new(Currency::Bitcoin)
        .description("test".into())
        .payment_hash(PaymentHash([hash_byte; 32]))
        .payment_secret(PaymentSecret([42; 32]))
        .current_timestamp()
        .expiry_time(Duration::from_secs(expiry_secs))
        .min_final_cltv_expiry_delta(144);
    if let Some(amount) = amount_msat {
        builder = builder.amount_milli_satoshis(amount);
    }
    builder
        .build_signed(|hash| Secp256k1::new().sign_ecdsa_recoverable(hash, &secret))
        .unwrap()
        .to_string()
}

fn hash_of(hash_byte: u8) -> String {
    hex_string(&[hash_byte; 32])
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn node_payment(id: &str, hash: &str, inbound: bool, status: PaymentStatus) -> NodePayment {
    NodePayment {
        payment_id: id.into(),
        payment_hash: Some(hash.into()),
        inbound,
        amount_msat: Some(1000),
        fee_paid_msat: Some(4),
        status,
        updated_at_secs: mesh_llm_wallet::now_ms() / 1000,
    }
}

fn error(kind: ApiErrorKind) -> ApiError {
    ApiError {
        kind,
        message: format!("{kind:?}"),
    }
}

#[tokio::test]
async fn issued_invoice_is_pending_until_paid_and_found_by_hash_after() {
    let h = harness();
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(1, Some(1000), 300));
    let invoice = h.provider.create_invoice(Some(1000), 300).await.unwrap();
    let hash = hash_of(1);
    assert_eq!(invoice.payment_hash, hash);

    let unpaid = h.provider.lookup(&hash).await.unwrap().unwrap();
    assert_eq!(unpaid.status, PaymentStatus::Pending);
    assert!(unpaid.inbound);
    assert!(!unpaid.claiming);

    // The node records the payment under an id that is not the hash.
    h.node.pages.lock().unwrap().insert(
        None,
        (
            vec![node_payment(
                "htlc-id",
                &hash,
                true,
                PaymentStatus::Succeeded,
            )],
            None,
        ),
    );
    h.node.store(node_payment(
        "htlc-id",
        &hash,
        true,
        PaymentStatus::Succeeded,
    ));
    let paid = h.provider.lookup(&hash).await.unwrap().unwrap();
    assert_eq!(paid.status, PaymentStatus::Succeeded);
    assert_eq!(paid.id, "htlc-id");

    // The id is cached, so the next lookup does not scan.
    h.node.calls.lock().unwrap().clear();
    h.provider.lookup(&hash).await.unwrap().unwrap();
    assert!(
        !h.node.calls().iter().any(|call| call.starts_with("page")),
        "{:?}",
        h.node.calls()
    );
}

#[tokio::test]
async fn created_invoice_must_honor_amount_and_expiry() {
    let h = harness();
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(2, Some(1000), 86_400));
    assert!(h.provider.create_invoice(Some(1000), 300).await.is_err());
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(3, Some(999), 300));
    assert!(h.provider.create_invoice(Some(1000), 300).await.is_err());
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(4, None, 300));
    let amountless = h.provider.create_invoice(None, 300).await.unwrap();
    assert_eq!(amountless.amount_msat, None);
    assert!(h.provider.create_invoice(Some(0), 300).await.is_err());
}

#[tokio::test]
async fn scanning_stops_at_payments_older_than_the_invoice() {
    let h = harness();
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(5, Some(1000), 300));
    h.provider.create_invoice(Some(1000), 300).await.unwrap();
    let mut old = node_payment("old", "ff", false, PaymentStatus::Succeeded);
    old.updated_at_secs = 1;
    h.node
        .pages
        .lock()
        .unwrap()
        .insert(None, (vec![old], Some("next".into())));
    let unpaid = h.provider.lookup(&hash_of(5)).await.unwrap().unwrap();
    assert_eq!(unpaid.status, PaymentStatus::Pending);
    assert!(
        !h.node.calls().contains(&"page:Some(\"next\")".to_string()),
        "{:?}",
        h.node.calls()
    );
}

#[tokio::test]
async fn pay_bounds_fees_by_the_host_cap_and_pays_once() {
    let h = harness();
    let invoice = Invoice::parse(&signed_invoice(6, Some(1000), 3600)).unwrap();
    let hash = hash_of(6);
    h.node
        .send_results
        .lock()
        .unwrap()
        .push_back(Ok(hash.clone()));
    // Not yet readable right after submission.
    let submitted = h.provider.pay(&invoice, 1000, 1250).await.unwrap();
    assert_eq!(submitted.status, PaymentStatus::Pending);
    assert_eq!(*h.node.sent.lock().unwrap(), [(None, 250)]);

    h.node
        .store(node_payment(&hash, &hash, false, PaymentStatus::Succeeded));
    let again = h.provider.pay(&invoice, 1000, 1250).await.unwrap();
    assert_eq!(again.status, PaymentStatus::Succeeded);
    assert_eq!(h.node.sent.lock().unwrap().len(), 1, "never pays twice");
}

#[tokio::test]
async fn amountless_invoice_is_sent_the_host_amount() {
    let h = harness();
    let invoice = Invoice::parse(&signed_invoice(7, None, 3600)).unwrap();
    h.node
        .send_results
        .lock()
        .unwrap()
        .push_back(Ok(hash_of(7)));
    h.provider.pay(&invoice, 700, 800).await.unwrap();
    assert_eq!(*h.node.sent.lock().unwrap(), [(Some(700), 100)]);
}

#[tokio::test]
async fn refusals_before_sending_are_not_submitted() {
    let h = harness();
    let invoice = Invoice::parse(&signed_invoice(8, Some(1000), 3600)).unwrap();

    let over_cap = h.provider.pay(&invoice, 1000, 999).await.unwrap_err();
    assert!(matches!(over_cap, PayError::NotSubmitted(_)), "{over_cap}");

    h.node
        .lookup_failures
        .lock()
        .unwrap()
        .push_back(error(ApiErrorKind::Uncertain));
    let lookup_down = h.provider.pay(&invoice, 1000, 1100).await.unwrap_err();
    assert!(
        matches!(lookup_down, PayError::NotSubmitted(_)),
        "{lookup_down}"
    );
    assert!(h.node.sent.lock().unwrap().is_empty());

    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(9, Some(1000), 300));
    let own = h.provider.create_invoice(Some(1000), 300).await.unwrap();
    let own_error = h.provider.pay(&own, 1000, 1100).await.unwrap_err();
    assert!(
        matches!(own_error, PayError::NotSubmitted(_)),
        "{own_error}"
    );
    assert!(h.node.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn send_failures_are_classified_by_what_ldk_could_have_done() {
    let invoice = Invoice::parse(&signed_invoice(10, Some(1000), 3600)).unwrap();
    let hash = hash_of(10);
    for (failure, record, not_submitted) in [
        (ApiErrorKind::InvalidRequest, None, true),
        (ApiErrorKind::Auth, None, true),
        (ApiErrorKind::Lightning, None, true),
        (ApiErrorKind::Lightning, Some(PaymentStatus::Failed), true),
        (ApiErrorKind::Lightning, Some(PaymentStatus::Pending), false),
        (ApiErrorKind::Uncertain, None, false),
    ] {
        let h = harness();
        h.node
            .send_results
            .lock()
            .unwrap()
            .push_back(Err(error(failure)));
        *h.node.recorded_on_send.lock().unwrap() =
            record.map(|status| node_payment(&hash, &hash, false, status));
        let error = h.provider.pay(&invoice, 1000, 1100).await.unwrap_err();
        assert_eq!(
            matches!(error, PayError::NotSubmitted(_)),
            not_submitted,
            "{failure:?} with {record:?}: {error}"
        );
    }
}

#[tokio::test]
async fn waits_wake_on_events_and_arrival_sees_claiming() {
    let h = harness();
    h.node
        .invoices
        .lock()
        .unwrap()
        .push_back(signed_invoice(11, Some(1000), 300));
    h.provider.create_invoice(Some(1000), 300).await.unwrap();
    let hash = hash_of(11);

    let node = Arc::clone(&h.node);
    let hash_for_node = hash.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let claiming = node_payment("htlc", &hash_for_node, true, PaymentStatus::Pending);
        node.pages
            .lock()
            .unwrap()
            .insert(None, (vec![claiming.clone()], None));
        node.store(claiming.clone());
        node.emit(claiming);
    });
    let arrived = tokio::time::timeout(Duration::from_secs(5), h.provider.wait_for_arrival(&hash))
        .await
        .expect("an event must wake the wait long before the poll")
        .unwrap();
    assert!(arrived.is_claiming());

    let node = Arc::clone(&h.node);
    let hash_for_node = hash.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let settled = node_payment("htlc", &hash_for_node, true, PaymentStatus::Succeeded);
        node.store(settled.clone());
        node.emit(settled);
    });
    let settled = tokio::time::timeout(Duration::from_secs(5), h.provider.wait_for_payment(&hash))
        .await
        .expect("an event must wake the wait long before the poll")
        .unwrap();
    assert_eq!(settled.status, PaymentStatus::Succeeded);
}

#[tokio::test]
async fn expired_unpaid_invoice_ends_as_failed() {
    let issued = IssuedInvoice {
        created_at_ms: 0,
        expires_at_ms: 1_000,
        amount_msat: Some(5),
        payment_id: None,
    };
    let within_grace = LdkProvider::unpaid("aa", &issued, 1_000 + EXPIRY_GRACE_MS);
    assert_eq!(within_grace.status, PaymentStatus::Pending);
    let expired = LdkProvider::unpaid("aa", &issued, 1_001 + EXPIRY_GRACE_MS);
    assert_eq!(expired.status, PaymentStatus::Failed);
}

#[tokio::test]
async fn transactions_page_until_the_limit() {
    let h = harness();
    let page = |ids: &[&str]| -> Vec<NodePayment> {
        ids.iter()
            .map(|id| node_payment(id, id, false, PaymentStatus::Succeeded))
            .collect()
    };
    {
        let mut pages = h.node.pages.lock().unwrap();
        pages.insert(None, (page(&["a", "b"]), Some("2".into())));
        pages.insert(Some("2".into()), (page(&["c", "d"]), Some("3".into())));
        pages.insert(Some("3".into()), (page(&["e"]), None));
    }
    let listed = h.provider.transactions(3).await.unwrap();
    let ids: Vec<&str> = listed.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c"]);
    assert!(!h.node.calls().contains(&"page:Some(\"3\")".to_string()));
}
