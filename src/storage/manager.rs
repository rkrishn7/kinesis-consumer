use crate::proto::KdsConsumer;

pub trait Manager {
    fn checkpoint_consumer(&self, sequence_number: &String, consumer: &KdsConsumer);
}
