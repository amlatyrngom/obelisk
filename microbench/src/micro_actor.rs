use common::{HandlerKit, ScalingState, ServerlessHandler};
use persistence::PersistentLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Request.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MicroActorReq {
    Increment(i64, i32),
    Retrieve,
}

/// Response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MicroActorResp {
    pub val: i64,
}

/// Actor.
pub struct MicroActor {
    plog: Arc<PersistentLog>,
    inner: Arc<RwLock<MicroActorInner>>,
    metadata: Vec<u8>,
}

/// Modifiable.
pub struct MicroActorInner {
    curr_value: i64,
}

/// Implementation.
impl MicroActor {
    /// Create actor.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            incarnation,
        } = kit;
        println!("Making micro bench actor: {}!", instance_info.identifier);
        let plog = Arc::new(
            PersistentLog::new(instance_info.clone(), incarnation)
                .await
                .unwrap(),
        );
        let inner = Arc::new(RwLock::new(MicroActorInner { curr_value: 0 }));
        let metadata = {
            let is_lambda = instance_info.private_url.is_none();
            let memory = instance_info.mem;
            let metadata = (memory, is_lambda);
            serde_json::to_vec(&metadata).unwrap()
        };
        let actor = MicroActor {
            plog,
            inner,
            metadata,
        };
        // Recover first.
        actor.recover().await;
        actor
    }

    /// Recover last flushed value.
    async fn recover(&self) {
        let flush_lsn = self.plog.wal.persist_lsn();
        let start_lsn = self.plog.wal.truncate_lsn();
        if flush_lsn < 0 || flush_lsn < start_lsn {
            // Nothing appended yet.
            return;
        }
        println!("Flush LSN: {flush_lsn}");
        println!("Start LSN: {start_lsn}");
        let mut entries = vec![];
        let mut replay_handle = None;
        let curr_value = tokio::task::block_in_place(|| {
            loop {
                let (new_handle, new_entries) = self.plog.wal.replay(replay_handle).unwrap();
                entries.extend(new_entries);
                if new_handle.is_none() {
                    break;
                }
                replay_handle = new_handle;
            }
            assert!(entries.len() > 0);
            let (entry, lsn) = entries.pop().unwrap();
            assert_eq!(lsn, flush_lsn);
            let curr_value: i64 = serde_json::from_slice(&entry).unwrap();
            self.plog.wal.truncate(flush_lsn - 1).unwrap();
            curr_value
        });
        {
            let mut inner = self.inner.write().await;
            inner.curr_value = curr_value;
        }
    }

    /// Increment.
    async fn handle_increment(&self, v: i64, caller_mem: i32) -> MicroActorResp {
        // Update and enqueue in log.
        let (new_value, lsn) = {
            let mut inner = self.inner.write().await;
            inner.curr_value += v;
            let entry: Vec<u8> = serde_json::to_vec(&inner.curr_value).unwrap();
            let lsn = self.plog.enqueue(entry, Some(caller_mem)).await;
            (inner.curr_value, lsn)
        };
        // Wait for flush and return.
        self.plog.flush_at(Some(lsn)).await;
        MicroActorResp { val: new_value }
    }

    /// Retrieve.
    async fn handle_retrieve(&self) -> MicroActorResp {
        // Just read the value.
        // For test. TODO: Remove
        let val = {
            let inner = self.inner.read().await;
            inner.curr_value
        };
        MicroActorResp { val }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for MicroActor {
    /// Handle a message.
    async fn handle(&self, msg: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: MicroActorReq = serde_json::from_str(&msg).unwrap();
        let resp: MicroActorResp = match req {
            MicroActorReq::Increment(v, caller_mem) => {
                if v == 0 {
                    self.handle_retrieve().await
                } else {
                    self.handle_increment(v, caller_mem).await
                }
            }
            MicroActorReq::Retrieve => self.handle_retrieve().await,
        };
        let resp: String = serde_json::to_string(&resp).unwrap();
        (resp, self.metadata.clone())
    }

    /// Checkpoint. Just truncate the log up to and excluding the last entry.
    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        println!("Checkpoint: Terminating=({terminating})");
        tokio::task::block_in_place(|| {
            let flush_lsn = self.plog.wal.persist_lsn();
            self.plog.wal.truncate(flush_lsn - 1).unwrap();
        });
        self.plog.check_replicas(terminating).await;
    }
}

#[cfg(test)]
mod test {

    use functional::FunctionalClient;
    use std::sync::Arc;

    use super::{MicroActorReq, MicroActorResp};

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_test() {
        run_basic_test().await;
    }

    async fn run_basic_test() {
        let caller_mem = 512;
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await);
        let req = MicroActorReq::Retrieve;
        let req = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&req, &vec![]).await.unwrap();
        let resp = serde_json::from_str::<MicroActorResp>(&resp).unwrap();
        println!("Resp: {resp:?}");
        let req = MicroActorReq::Increment(10 - resp.val, caller_mem);
        let req = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&req, &vec![]).await.unwrap();
        let resp = serde_json::from_str::<MicroActorResp>(&resp).unwrap();
        println!("Resp: {resp:?}");
        let req = MicroActorReq::Increment(-5, caller_mem);
        let req = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&req, &vec![]).await.unwrap();
        let resp = serde_json::from_str::<MicroActorResp>(&resp).unwrap();
        println!("Resp: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_scaling_test() {
        run_basic_scaling_test(false).await;
    }

    async fn run_basic_scaling_test(read: bool) {
        let mem = 32768; // Large memory.
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microactor", Some(0), Some(mem)).await);
        let req = if read {
            MicroActorReq::Retrieve
        } else {
            MicroActorReq::Increment(1, mem)
        };
        let req = serde_json::to_string(&req).unwrap();
        for _ in 0..5000 {
            let start_time = std::time::Instant::now();
            let resp = fc.invoke(&req, &vec![]).await;
            match resp {
                Ok((resp, _)) => {
                    let resp = serde_json::from_str::<MicroActorResp>(&resp).unwrap();
                    println!("Resp: {resp:?}. Duration={:?}.", start_time.elapsed());
                }
                Err(e) => {
                    println!("Error: {e}");
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn updown_test() {
        run_updown_test(false).await;
    }

    async fn run_updown_test(read: bool) {
        let up_mem = 16384; // Large memory.
        let down_mem = 8; // Small memory.
        let up_reqs = if read { 2000 } else { 5000 };
        let down_reqs = 400;
        for up in [true, false] {
            let mem = if up { up_mem } else { down_mem };
            // Maintain high memory to prevent actor downscale before replica downscale.
            let fc_mem = if !read { up_mem } else { mem };
            let fc = Arc::new(
                FunctionalClient::new("microbench", "microactor", Some(0), Some(fc_mem)).await,
            );
            let req = if read {
                MicroActorReq::Retrieve
            } else {
                MicroActorReq::Increment(1, mem)
            };
            let req = serde_json::to_string(&req).unwrap();
            let num_reqs = if up { up_reqs } else { down_reqs };
            let sleep_time_secs = if up { 0 } else { 1 };
            for _ in 0..num_reqs {
                let start_time = std::time::Instant::now();
                let resp = fc.invoke(&req, &vec![]).await;
                match resp {
                    Ok((resp, _)) => {
                        let resp = serde_json::from_str::<MicroActorResp>(&resp).unwrap();
                        println!("Resp: {resp:?}. Duration={:?}.", start_time.elapsed());
                    }
                    Err(e) => {
                        println!("Error: {e}");
                    }
                }
                if sleep_time_secs > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(sleep_time_secs)).await;
                }
            }
        }
    }
}
