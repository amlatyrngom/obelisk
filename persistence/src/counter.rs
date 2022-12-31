use super::PersistentLog;
use common::ActorInstance;
use std::sync::Arc;

pub struct Counter {
    plog: Arc<PersistentLog>,
}

impl Counter {
    /// Create echo actor.
    pub async fn new(_name: &str, plog: Arc<PersistentLog>) -> Self {
        println!("Making counter actor!");
        Counter { plog }
    }
}

#[async_trait::async_trait]
impl ActorInstance for Counter {
    async fn message(&self, _msg: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        // Make flushes and time them.
        let flush_lsn = self.plog.get_flush_lsn().await;
        self.plog.truncate(flush_lsn).await;
        let start_time = std::time::Instant::now();
        for _ in 0..10 {
            self.plog.enqueue(vec![1]).await;
            self.plog.flush(None).await;
            println!("Done Flushing");
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time).div_f64(10.0);
        println!("Flushes took: {duration:?}");
        let duration_ms = duration.as_millis();
        (duration_ms.to_string(), vec![])
    }

    async fn checkpoint(&self, terminating: bool) {
        println!("Checkpoint: terminating=({terminating})");
        if terminating {
            let flush_lsn = self.plog.get_flush_lsn().await;
            self.plog.truncate(flush_lsn).await;
        }
    }
}
