use common::adaptation::{Rescaler, ServerfulScalingState};
// use common::adaptation::deploy::DeploymentInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.25;

/// Info to maintain for scaling functions.
#[derive(Serialize, Deserialize)]
struct LogScalingInfo {
    activity: f64,
}

/// Rescaler for functions.
pub struct LogRescaler {}

impl LogRescaler {
    /// Initialize. Nothing to do.
    pub async fn new() -> Self {
        LogRescaler {}
    }
}

#[async_trait::async_trait]
impl Rescaler for LogRescaler {
    /// Just compute a moving average.
    async fn rescale(
        &self,
        scaling_state: &ServerfulScalingState,
        curr_timestamp: chrono::DateTime<chrono::Utc>,
        metrics: Vec<Value>,
    ) -> (u64, Value) {
        // Get memory size.
        // let mem_size = serde_json::from_value(scaling_state.deployment.clone())
        //     .map_or(2048.0, |v: DeploymentInfo| v.msg_info.unwrap().mem as f64);
        // Get old activity.
        let mut activity = if let Some(scaling_info) = &scaling_state.scaling_info {
            let scaling_info: LogScalingInfo =
                serde_json::from_value(scaling_info.clone()).unwrap();
            scaling_info.activity
        } else {
            0.0
        };
        // Compute the total activity of the new metrics.
        let total_interval = curr_timestamp
            .signed_duration_since(scaling_state.last_rescale)
            .num_seconds() as f64;
        // Assume every flush increases latency by 10ms.
        let mut total_active_secs: f64 = 0.0;
        println!("Num metrics: {}", metrics.len());
        for _ in metrics.iter() {
            total_active_secs += 0.01;
        }
        // Compute moving average.
        let new_activity = total_active_secs / total_interval;
        activity = (1.0 - MOVING_FACTOR) * activity + MOVING_FACTOR * new_activity;
        // Compute price ratio.
        // let ecs_cost = 6.0 * (0.012144 + 2.0 * 0.0013335); // Cost of 6 nodes with 1vcpu and 2GB of RAM.
        // let mem_size = mem_size / 1024.0;
        // let lambda_cost = mem_size * 0.0000166667 * 3600.0; // Cost of running a lambda.
        // let price_ratio = ecs_cost / lambda_cost;
        // Set new scale.
        let new_scale = if activity >= 0.1 { 6 } else { 0 };
        let new_scaling_info = LogScalingInfo { activity };
        (new_scale, serde_json::to_value(&new_scaling_info).unwrap())
    }
}
