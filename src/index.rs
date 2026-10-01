//! Invoices this wallet issued.
//!
//! LDK Server has no record of an invoice until money arrives for it, and then
//! keys the payment by an HTLC-derived id rather than the payment hash. This
//! index remembers what was issued, so a lookup can tell "ours, unpaid" from
//! "unknown", and caches the payment id once the node reports one.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const FILE_NAME: &str = "issued-invoices.json";
/// Entries are kept this long after their invoice expired, well past any
/// payment that could still be settling.
const RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssuedInvoice {
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount_msat: Option<u64>,
    /// The node's id for the incoming payment, once seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_id: Option<String>,
}

pub struct InvoiceIndex {
    path: PathBuf,
    entries: Mutex<BTreeMap<String, IssuedInvoice>>,
}

impl InvoiceIndex {
    pub fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("create wallet directory {}", directory.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let path = directory.join(FILE_NAME);
        let entries = match std::fs::read(&path) {
            Ok(raw) => serde_json::from_slice(&raw)
                .with_context(|| format!("{} is unreadable", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", path.display()));
            }
        };
        Ok(Self {
            path,
            entries: Mutex::new(entries),
        })
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, IssuedInvoice>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn get(&self, payment_hash: &str) -> Option<IssuedInvoice> {
        self.entries().get(payment_hash).cloned()
    }

    /// Record an issued invoice durably. Callers must not hand the invoice out
    /// before this returns, or a restart could forget it was ours.
    pub fn insert(&self, payment_hash: &str, invoice: IssuedInvoice, now_ms: u64) -> Result<()> {
        let mut entries = self.entries();
        entries.retain(|_, entry| entry.expires_at_ms.saturating_add(RETENTION_MS) > now_ms);
        entries.insert(payment_hash.to_owned(), invoice);
        self.persist(&entries)
    }

    /// Remember the node's payment id for an issued invoice. Best effort: the
    /// id can always be found again by scanning payments.
    pub fn record_payment_id(&self, payment_hash: &str, payment_id: &str) {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(payment_hash) else {
            return;
        };
        if entry.payment_id.as_deref() == Some(payment_id) {
            return;
        }
        entry.payment_id = Some(payment_id.to_owned());
        if let Err(error) = self.persist(&entries) {
            tracing::debug!(target: "wallet_ldk_server", %error, "could not cache payment id");
        }
    }

    fn persist(&self, entries: &BTreeMap<String, IssuedInvoice>) -> Result<()> {
        let staging = self
            .path
            .with_extension(format!("{}.tmp", std::process::id()));
        std::fs::write(&staging, serde_json::to_vec_pretty(entries)?)?;
        OpenOptions::new().write(true).open(&staging)?.sync_all()?;
        std::fs::rename(&staging, &self.path)?;
        #[cfg(unix)]
        if let Some(parent) = self.path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issued(expires_at_ms: u64) -> IssuedInvoice {
        IssuedInvoice {
            created_at_ms: 1,
            expires_at_ms,
            amount_msat: Some(1000),
            payment_id: None,
        }
    }

    #[test]
    fn entries_survive_reopening_and_payment_ids_are_cached() {
        let dir = tempfile::tempdir().unwrap();
        let index = InvoiceIndex::open(dir.path()).unwrap();
        assert_eq!(index.get("aa"), None);
        index.insert("aa", issued(10_000), 0).unwrap();
        index.record_payment_id("aa", "id-1");
        index.record_payment_id("unknown", "id-2");

        let reopened = InvoiceIndex::open(dir.path()).unwrap();
        let entry = reopened.get("aa").unwrap();
        assert_eq!(entry.payment_id.as_deref(), Some("id-1"));
        assert_eq!(reopened.get("unknown"), None);
    }

    #[test]
    fn long_expired_entries_are_pruned_on_insert() {
        let dir = tempfile::tempdir().unwrap();
        let index = InvoiceIndex::open(dir.path()).unwrap();
        index.insert("old", issued(1), 0).unwrap();
        index
            .insert("new", issued(RETENTION_MS + 10), RETENTION_MS + 5)
            .unwrap();
        assert_eq!(index.get("old"), None);
        assert!(index.get("new").is_some());
    }

    #[test]
    fn a_corrupt_index_is_an_error_not_an_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE_NAME), b"{not json").unwrap();
        assert!(InvoiceIndex::open(dir.path()).is_err());
    }
}
