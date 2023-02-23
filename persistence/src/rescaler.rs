use common::adaptation::deploy::DeploymentInfo;
use common::adaptation::{Rescaler, ServerfulScalingState};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.1;
/// Hourly cost of 1vcpu + 2GB RAM.
const ECS_BASE_COST: f64 = 0.015;

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
        let caller_mem = serde_json::from_value(scaling_state.deployment.clone())
            .map_or(512.0, |v: DeploymentInfo| {
                v.msg_info.unwrap().caller_mem as f64
            })
            / 1024.0;
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
        // Assume every flush increases caller's latency by 10ms.
        let mut total_active_secs: f64 = 0.0;
        println!("Num metrics: {}", metrics.len());
        for _ in metrics.iter() {
            total_active_secs += 0.010;
        }
        // Compute moving average.
        let new_activity = total_active_secs / total_interval;
        activity = (1.0 - MOVING_FACTOR) * activity + MOVING_FACTOR * new_activity;
        // Compute price ratio.
        let ecs_cost = 6.0 * ECS_BASE_COST / 0.5; // Hourly cost of 6 nodes with 0.5vcpu and 1GB of RAM.
        let caller_lambda_cost = activity * caller_mem * 0.0000166667 * 3600.0; // Hourly cost of running a lambda.
                                                                                // Set new scale.
        let new_scale = if ecs_cost < caller_lambda_cost { 6 } else { 0 };
        let new_scaling_info = LogScalingInfo { activity };
        (new_scale, serde_json::to_value(&new_scaling_info).unwrap())
    }
}
