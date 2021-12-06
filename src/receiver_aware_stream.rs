//! Asynchronous stream with a notification-based drop implementation.

use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio_stream::Stream;

/// A wrapper around `tokio::sync::mpsc::Receiver` that
/// sends a single message to a `tokio::sync::oneshot`
/// channel when it is dropped.
pub struct ReceiverAwareStream<T> {
    /// The wrapped `Receiver`
    inner: Receiver<T>,
    /// `oneshot::Sender` for drop notification
    notifier: Option<Sender<()>>,
}

impl<T> ReceiverAwareStream<T> {
    pub fn new(receiver: Receiver<T>, notifier: Option<Sender<()>>) -> Self {
        Self {
            inner: receiver,
            notifier,
        }
    }
}

impl<T> Stream for ReceiverAwareStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> Drop for ReceiverAwareStream<T> {
    fn drop(&mut self) {
        if let Some(notifier) = self.notifier.take() {
            let _ = notifier.send(());
        }
    }
}
