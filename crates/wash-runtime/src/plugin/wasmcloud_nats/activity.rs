//! Completion accounting for Core NATS deliveries, including queued work.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::{Notify, mpsc, oneshot};

#[derive(Default)]
pub(super) struct Activity {
    pending: AtomicUsize,
    generation: AtomicU64,
    changed: Notify,
    readers: Mutex<Vec<mpsc::Sender<oneshot::Sender<()>>>>,
}

pub(super) struct Delivery(pub Arc<Activity>);
impl Drop for Delivery {
    fn drop(&mut self) {
        self.0.generation.fetch_add(1, Ordering::SeqCst);
        self.0.pending.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}
impl Activity {
    pub fn begin(self: &Arc<Self>) -> Delivery {
        self.pending.fetch_add(1, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        Delivery(self.clone())
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
    pub fn reader(&self) -> mpsc::Receiver<oneshot::Sender<()>> {
        let (tx, rx) = mpsc::channel(1);
        self.readers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tx);
        rx
    }
    pub async fn fence(&self) -> anyhow::Result<()> {
        let readers = self
            .readers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for reader in readers {
            let (tx, rx) = oneshot::channel();
            reader
                .send(tx)
                .await
                .map_err(|_| anyhow::anyhow!("NATS subscription reader stopped"))?;
            rx.await
                .map_err(|_| anyhow::anyhow!("NATS subscription fence cancelled"))?;
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pending.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            changed.await;
        }
    }
}

/// Waits for the broker to process commands previously sent by this client.
/// A local buffer flush does not provide this ordering guarantee.
pub async fn synchronize(client: &async_nats::Client) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use futures::StreamExt as _;
    let inbox = client.new_inbox();
    let mut sentinel = client.subscribe(inbox.clone()).await?;
    client.publish(inbox, bytes::Bytes::new()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), sentinel.next())
        .await
        .context("NATS synchronization timed out")?
        .context("NATS synchronization subscription closed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn fence_waits_for_queued_delivery_until_its_owner_finishes() {
        let activity = Arc::new(Activity::default());
        let delivery = activity.begin();
        let mut reader = activity.reader();
        let fence = tokio::spawn({
            let activity = activity.clone();
            async move { activity.fence().await }
        });
        reader.recv().await.unwrap().send(()).unwrap();
        assert!(!fence.is_finished());
        drop(delivery);
        tokio::time::timeout(Duration::from_secs(1), fence)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stopped_reader_fails_instead_of_reporting_idle() {
        let activity = Activity::default();
        drop(activity.reader());
        assert!(activity.fence().await.is_err());
    }
}
