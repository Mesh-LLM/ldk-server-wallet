//! The slice of LDK Server this wallet uses, behind a trait so the provider's
//! money-moving rules can be tested without a node.

use std::time::Duration;

use async_trait::async_trait;
use ldk_server_client::client::{EventStream, LdkServerClient};
use ldk_server_client::error::{LdkServerError, LdkServerErrorCode};
use ldk_server_client::ldk_server_grpc::api::{
    Bolt11ReceiveRequest, Bolt11SendRequest, GetNodeInfoRequest, GetPaymentDetailsRequest,
    ListChannelsRequest, ListPaymentsRequest,
};
use ldk_server_client::ldk_server_grpc::events::event_envelope::Event;
use ldk_server_client::ldk_server_grpc::types::{
    self, Bolt11InvoiceDescription, PaymentDirection, RouteParametersConfig,
    bolt11_invoice_description, payment_kind,
};
use mesh_llm_wallet::provider::PaymentStatus;

/// A payment as the node reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePayment {
    pub payment_id: String,
    /// Set for BOLT11 payments, the only kind this wallet creates or pays.
    pub payment_hash: Option<String>,
    pub inbound: bool,
    pub amount_msat: Option<u64>,
    pub fee_paid_msat: Option<u64>,
    pub status: PaymentStatus,
    pub updated_at_secs: u64,
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    /// As [`mesh_llm_wallet::contract::WalletIdentity::network`] names it.
    pub network: &'static str,
}

/// How a request failed, keeping what matters for "could money have moved".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiErrorKind {
    /// The server rejected the request as malformed. Nothing was attempted.
    InvalidRequest,
    /// The macaroon was refused or lacks the permission. Nothing was attempted.
    Auth,
    /// LDK refused the operation before acting on it: route not found,
    /// duplicate payment, insufficient funds.
    Lightning,
    /// The server failed after accepting the request, or the transport broke
    /// or timed out. The operation may have happened.
    Uncertain,
}

#[derive(Clone, Debug)]
pub struct ApiError {
    pub kind: ApiErrorKind,
    pub message: String,
}

impl ApiError {
    pub fn uncertain(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::Uncertain,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ApiError {}

impl From<LdkServerError> for ApiError {
    fn from(error: LdkServerError) -> Self {
        let kind = match error.error_code {
            LdkServerErrorCode::InvalidRequestError => ApiErrorKind::InvalidRequest,
            LdkServerErrorCode::AuthError | LdkServerErrorCode::AuthorizationError => {
                ApiErrorKind::Auth
            }
            LdkServerErrorCode::LightningError => ApiErrorKind::Lightning,
            LdkServerErrorCode::InternalServerError | LdkServerErrorCode::InternalError => {
                ApiErrorKind::Uncertain
            }
        };
        Self {
            kind,
            message: error.message,
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

/// A live event subscription.
#[async_trait]
pub trait PaymentEvents: Send {
    /// The next payment event, `Ok(None)` for events about something else, or
    /// `None` once the stream has ended.
    async fn next(&mut self) -> Option<ApiResult<Option<NodePayment>>>;
}

#[async_trait]
pub trait NodeApi: Send + Sync + 'static {
    async fn node_info(&self) -> ApiResult<NodeInfo>;
    /// What the node could send right now across its usable channels.
    async fn sendable_msat(&self) -> ApiResult<u64>;
    async fn create_invoice(
        &self,
        amount_msat: Option<u64>,
        description: &str,
        expiry_secs: u32,
    ) -> ApiResult<String>;
    /// Start paying `bolt11` with LDK bounding total routing fees at
    /// `max_fee_msat` across every attempt. Returns the payment id.
    async fn send(
        &self,
        bolt11: &str,
        amount_msat: Option<u64>,
        max_fee_msat: u64,
    ) -> ApiResult<String>;
    async fn payment(&self, payment_id: &str) -> ApiResult<Option<NodePayment>>;
    /// One page of payments, newest first, and the token for the next page.
    async fn payments_page(
        &self,
        page_token: Option<String>,
    ) -> ApiResult<(Vec<NodePayment>, Option<String>)>;
    async fn subscribe(&self) -> ApiResult<Box<dyn PaymentEvents>>;
}

/// [`NodeApi`] over the real client. Every call has a deadline because the
/// client sets none; a timed-out call is uncertain.
pub struct ServerApi {
    client: LdkServerClient,
    request_timeout: Duration,
    pay_timeout: Duration,
}

impl ServerApi {
    pub fn new(client: LdkServerClient, request_timeout: Duration, pay_timeout: Duration) -> Self {
        Self {
            client,
            request_timeout,
            pay_timeout,
        }
    }
}

async fn deadline<T>(
    timeout: Duration,
    call: impl std::future::Future<Output = Result<T, LdkServerError>>,
) -> ApiResult<T> {
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result.map_err(ApiError::from),
        Err(_) => Err(ApiError::uncertain(format!(
            "no response within {timeout:?}"
        ))),
    }
}

/// The node's network, named as a wallet identity names it.
pub fn network_from_proto(network: i32) -> Option<&'static str> {
    Some(match types::Network::from_i32(network)? {
        types::Network::Bitcoin => "mainnet",
        types::Network::Testnet => "testnet",
        types::Network::Testnet4 => "testnet4",
        types::Network::Signet => "signet",
        types::Network::Regtest => "regtest",
    })
}

pub fn payment_from_proto(payment: types::Payment) -> NodePayment {
    let payment_hash = match payment.kind.and_then(|kind| kind.kind) {
        Some(payment_kind::Kind::Bolt11(bolt11)) => Some(bolt11.hash),
        _ => None,
    };
    let status = match types::PaymentStatus::from_i32(payment.status) {
        Some(types::PaymentStatus::Succeeded) => PaymentStatus::Succeeded,
        Some(types::PaymentStatus::Failed) => PaymentStatus::Failed,
        // Unknown future statuses are not terminal as far as this wallet
        // can tell.
        Some(types::PaymentStatus::Pending) | None => PaymentStatus::Pending,
    };
    NodePayment {
        payment_id: payment.payment_id,
        payment_hash,
        inbound: payment.direction == PaymentDirection::Inbound as i32,
        amount_msat: payment.amount_msat,
        fee_paid_msat: payment.fee_paid_msat,
        status,
        updated_at_secs: payment.latest_update_timestamp,
    }
}

#[async_trait]
impl NodeApi for ServerApi {
    async fn node_info(&self) -> ApiResult<NodeInfo> {
        let info = deadline(
            self.request_timeout,
            self.client.get_node_info(GetNodeInfoRequest {}),
        )
        .await?;
        let network = network_from_proto(info.network).ok_or_else(|| ApiError {
            kind: ApiErrorKind::InvalidRequest,
            message: format!("unknown network {}", info.network),
        })?;
        Ok(NodeInfo {
            node_id: info.node_id,
            network,
        })
    }

    async fn sendable_msat(&self) -> ApiResult<u64> {
        let channels = deadline(
            self.request_timeout,
            self.client.list_channels(ListChannelsRequest {}),
        )
        .await?;
        Ok(channels
            .channels
            .iter()
            .filter(|channel| channel.is_usable)
            .map(|channel| channel.next_outbound_htlc_limit_msat)
            .fold(0u64, u64::saturating_add))
    }

    async fn create_invoice(
        &self,
        amount_msat: Option<u64>,
        description: &str,
        expiry_secs: u32,
    ) -> ApiResult<String> {
        let response = deadline(
            self.request_timeout,
            self.client.bolt11_receive(Bolt11ReceiveRequest {
                amount_msat,
                description: Some(Bolt11InvoiceDescription {
                    kind: Some(bolt11_invoice_description::Kind::Direct(
                        description.to_owned(),
                    )),
                }),
                expiry_secs,
            }),
        )
        .await?;
        Ok(response.invoice)
    }

    async fn send(
        &self,
        bolt11: &str,
        amount_msat: Option<u64>,
        max_fee_msat: u64,
    ) -> ApiResult<String> {
        // Supplying route parameters replaces every default, so the rest are
        // filled with the client's documented defaults.
        let route_parameters = RouteParametersConfig {
            max_total_routing_fee_msat: Some(max_fee_msat),
            max_total_cltv_expiry_delta: ldk_server_client::DEFAULT_MAX_TOTAL_CLTV_EXPIRY_DELTA,
            max_path_count: ldk_server_client::DEFAULT_MAX_PATH_COUNT,
            max_channel_saturation_power_of_half:
                ldk_server_client::DEFAULT_MAX_CHANNEL_SATURATION_POWER_OF_HALF,
        };
        let response = deadline(
            self.pay_timeout,
            self.client.bolt11_send(Bolt11SendRequest {
                invoice: bolt11.to_owned(),
                amount_msat,
                route_parameters: Some(route_parameters),
            }),
        )
        .await?;
        Ok(response.payment_id)
    }

    async fn payment(&self, payment_id: &str) -> ApiResult<Option<NodePayment>> {
        let response = deadline(
            self.request_timeout,
            self.client.get_payment_details(GetPaymentDetailsRequest {
                payment_id: payment_id.to_owned(),
            }),
        )
        .await?;
        Ok(response.payment.map(payment_from_proto))
    }

    async fn payments_page(
        &self,
        page_token: Option<String>,
    ) -> ApiResult<(Vec<NodePayment>, Option<String>)> {
        let response = deadline(
            self.request_timeout,
            self.client
                .list_payments(ListPaymentsRequest { page_token }),
        )
        .await?;
        Ok((
            response
                .payments
                .into_iter()
                .map(payment_from_proto)
                .collect(),
            response.next_page_token,
        ))
    }

    async fn subscribe(&self) -> ApiResult<Box<dyn PaymentEvents>> {
        let stream = deadline(self.request_timeout, self.client.subscribe_events()).await?;
        Ok(Box::new(ServerEvents(stream)))
    }
}

struct ServerEvents(EventStream);

#[async_trait]
impl PaymentEvents for ServerEvents {
    async fn next(&mut self) -> Option<ApiResult<Option<NodePayment>>> {
        let envelope = match self.0.next_message().await? {
            Ok(envelope) => envelope,
            Err(error) => return Some(Err(error.into())),
        };
        let payment = match envelope.event {
            Some(Event::PaymentReceived(event)) => event.payment,
            Some(Event::PaymentSuccessful(event)) => event.payment,
            Some(Event::PaymentFailed(event)) => event.payment,
            Some(Event::PaymentClaimable(event)) => event.payment,
            _ => None,
        };
        Some(Ok(payment.map(payment_from_proto)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bolt11_payment(direction: PaymentDirection, status: types::PaymentStatus) -> types::Payment {
        types::Payment {
            payment_id: "id".into(),
            kind: Some(types::PaymentKind {
                kind: Some(payment_kind::Kind::Bolt11(types::Bolt11 {
                    hash: "aa".into(),
                    ..Default::default()
                })),
            }),
            amount_msat: Some(1000),
            fee_paid_msat: Some(3),
            direction: direction as i32,
            status: status as i32,
            latest_update_timestamp: 7,
        }
    }

    #[test]
    fn payments_map_direction_status_and_hash() {
        let inbound = payment_from_proto(bolt11_payment(
            PaymentDirection::Inbound,
            types::PaymentStatus::Pending,
        ));
        assert!(inbound.inbound);
        assert_eq!(inbound.status, PaymentStatus::Pending);
        assert_eq!(inbound.payment_hash.as_deref(), Some("aa"));

        let outbound = payment_from_proto(bolt11_payment(
            PaymentDirection::Outbound,
            types::PaymentStatus::Succeeded,
        ));
        assert!(!outbound.inbound);
        assert_eq!(outbound.status, PaymentStatus::Succeeded);
        assert_eq!(outbound.fee_paid_msat, Some(3));

        let mut unknown = bolt11_payment(PaymentDirection::Outbound, types::PaymentStatus::Failed);
        unknown.status = 99;
        assert_eq!(payment_from_proto(unknown).status, PaymentStatus::Pending);
    }

    #[test]
    fn error_codes_keep_the_money_moved_distinction() {
        let kind = |code| ApiError::from(LdkServerError::new(code, "x")).kind;
        assert_eq!(
            kind(LdkServerErrorCode::LightningError),
            ApiErrorKind::Lightning
        );
        assert_eq!(kind(LdkServerErrorCode::AuthError), ApiErrorKind::Auth);
        assert_eq!(
            kind(LdkServerErrorCode::InternalServerError),
            ApiErrorKind::Uncertain
        );
        assert_eq!(
            kind(LdkServerErrorCode::InternalError),
            ApiErrorKind::Uncertain
        );
    }

    #[test]
    fn networks_map_onto_wallet_identity_names() {
        assert_eq!(
            network_from_proto(types::Network::Bitcoin as i32),
            Some("mainnet")
        );
        assert_eq!(
            network_from_proto(types::Network::Testnet4 as i32),
            Some("testnet4")
        );
        assert_eq!(network_from_proto(42), None);
    }
}
