use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio_stream::Stream;

pub struct ReceiverAwareStream<T> {
    inner: Receiver<T>,
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
            match notifier.send(()) {
                Ok(_) => (),
                Err(e) => println!("Error notifying dropped receiver"),
            }
        }
    }
}
