//! Payment events from the node, used only to wake settlement waits.
//!
//! LDK Server delivers events best effort: nothing is replayed, events are
//! dropped while no subscriber is connected, and a lagging subscriber skips
//! events silently. So an event never decides an outcome. It only tells a
//! waiter to look the payment up now instead of at its next backstop poll,
//! and every reconnect wakes all waiters because anything may have happened
//! while the stream was down.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::api::NodeApi;
use crate::index::InvoiceIndex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wake {
    /// Something happened to the payment with this hash.
    Payment(String),
    /// The stream (re)connected; every waiter should look again.
    Resync,
}

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct EventFeed {
    wake: broadcast::Sender<Wake>,
    connected: watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl EventFeed {
    pub fn spawn(api: Arc<dyn NodeApi>, index: Arc<InvoiceIndex>) -> Self {
        let (wake, _) = broadcast::channel(256);
        let (connected_tx, connected) = watch::channel(false);
        let task = tokio::spawn(follow(api, index, wake.clone(), connected_tx));
        Self {
            wake,
            connected,
            task,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Wake> {
        self.wake.subscribe()
    }

    /// Whether events are currently flowing. Waiters poll faster when not.
    pub fn is_connected(&self) -> bool {
        *self.connected.borrow()
    }
}

impl Drop for EventFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn follow(
    api: Arc<dyn NodeApi>,
    index: Arc<InvoiceIndex>,
    wake: broadcast::Sender<Wake>,
    connected: watch::Sender<bool>,
) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match api.subscribe().await {
            Ok(mut events) => {
                backoff = INITIAL_BACKOFF;
                let _ = connected.send(true);
                let _ = wake.send(Wake::Resync);
                while let Some(event) = events.next().await {
                    match event {
                        Ok(Some(payment)) => {
                            let Some(hash) = payment.payment_hash else {
                                continue;
                            };
                            if payment.inbound {
                                index.record_payment_id(&hash, &payment.payment_id);
                            }
                            let _ = wake.send(Wake::Payment(hash));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::debug!(target: "wallet_ldk_server", %error, "event stream error");
                            break;
                        }
                    }
                }
                let _ = connected.send(false);
            }
            Err(error) => {
                tracing::debug!(target: "wallet_ldk_server", %error, "event subscription failed");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
