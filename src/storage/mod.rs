//! QuestDB writer.

mod questdb;

pub use questdb::{
    QuestDbHandle, QuestDbWriter, StorageEvent, StorageSendResult, Variant, WriterMetrics,
};
