use common::{HandlerKit, InstanceInfo, ScalingState, ServerlessHandler};
use std::sync::Arc;

pub struct Echo {
    instance_info: Arc<InstanceInfo>,
}

impl Echo {
    /// Create echo handler.
    pub async fn new(kit: HandlerKit) -> Self {
        println!("Creating echo function: {:?}", kit.instance_info);
        Echo {
            instance_info: kit.instance_info,
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for Echo {
    /// Handle message.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        println!("Echo Handler: {:?}. Meta={meta}", self.instance_info);
        if let Ok(sleep_time_secs) = meta.parse::<f64>() {
            tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_time_secs)).await;
        } else if let Ok((sleep_time_secs, compute_time_secs)) =
            serde_json::from_str::<(f64, f64)>(&meta)
        {
            let num_threads = 4;
            let mut ts = Vec::new();
            for i in 0..num_threads {
                ts.push(tokio::task::spawn_blocking(move || {
                    let start_time = std::time::Instant::now();
                    let mut x: i64 = 1000;
                    loop {
                        let curr_time = std::time::Instant::now();
                        let since_secs = curr_time.duration_since(start_time).as_secs_f64();
                        if since_secs >= compute_time_secs {
                            break;
                        }
                        for i in 0..1_000_000 {
                            x += i % x;
                        }
                    }
                    println!("Thread {i}. Computed {x}.");
                }));
            }
            for t in ts {
                let _ = t.await;
            }
            let sleep_time = std::time::Duration::from_secs_f64(sleep_time_secs);
            tokio::time::sleep(sleep_time).await;
        };
        (meta, payload)
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        println!("Echo Handler Checkpointing: {scaling_state:?}");
        if terminating {
            println!("Echo Handler Terminating.");
        }
    }
}
