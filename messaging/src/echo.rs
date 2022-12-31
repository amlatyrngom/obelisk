use common::ActorInstance;
use persistence::PersistentLog;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub struct Echo {
    name: String,
    _plog: Arc<PersistentLog>,
    num_calls: AtomicU64,
}

impl Echo {
    /// Create echo actor.
    pub async fn new(name: &str, plog: Arc<PersistentLog>) -> Self {
        println!("Making echo actor!");
        Echo {
            name: name.into(),
            _plog: plog,
            num_calls: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ActorInstance for Echo {
    async fn message(&self, msg: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        let curr_num_calls = self.num_calls.fetch_add(1, Ordering::AcqRel);
        println!(
            "Echo {}. Received msg({msg}), payload_length=({}), num_calls=({curr_num_calls})",
            self.name,
            payload.len()
        );
        (msg, payload)
    }

    async fn checkpoint(&self, terminating: bool) {
        println!("Checkpoint: terminating=({terminating})");
    }
}
