use common::{HandlerScalingState, Rescaler, RescalingResult, SubsystemScalingState};
use serde::{Deserialize, Serialize};

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.25;
/// Replica vcpus.
const REPLICAS_VCPUS: f64 = 0.25;
/// Replics mem.
const REPLICAS_MEM_GB: f64 = 1.0;
/// Time saved with replication.
const REPLICATION_GAIN_SECS: f64 = 0.010;

/// Info to maintain for scaling functions.
#[derive(Serialize, Deserialize, Debug)]
struct WalScalingInfo {
    user_activity_gbsec: f64, // Rate of duration * caller_mem.
}

#[derive(Serialize, Deserialize)]
pub struct WalMetric {
    pub user_mems: Vec<i32>,
}

/// Rescaler for functions.
pub struct WalRescaler {}

#[async_trait::async_trait]
impl Rescaler for WalRescaler {
    /// Just compute a moving average.
    async fn rescale(
        &self,
        subsystem_scaling_state: &SubsystemScalingState,
        _handler_scaling_state: Option<&HandlerScalingState>,
        since_last_rescale: std::time::Duration,
        metrics: Vec<Vec<u8>>,
    ) -> RescalingResult {
        // Make default rescaling result.
        let mut rescaling_result = RescalingResult {
            services_scales: subsystem_scaling_state.service_scales.clone(),
            services_scaling_info: subsystem_scaling_state.scaling_info.clone(),
            handler_scales: None,
            handler_scaling_info: None,
        };
        // Read current stats or make new ones.
        let mut current_stats = subsystem_scaling_state.scaling_info.as_ref().map_or(
            WalScalingInfo {
                user_activity_gbsec: 0.0,
            },
            |s| serde_json::from_str(s).unwrap(),
        );
        // Update moving averages.
        self.update_moving_averages(
            &mut current_stats,
            metrics,
            since_last_rescale.as_secs_f64(),
        );
        let (replicas_price, user_gains) = self.summarize_stats(&current_stats);
        println!("WalRescaler. RP={replicas_price}. UG={user_gains}. CS={current_stats:?}");
        let to_deploy = if replicas_price < user_gains { 2 } else { 0 };
        rescaling_result
            .services_scales
            .insert("replica".into(), to_deploy);
        rescaling_result
    }
}

impl WalRescaler {
    /// Initialize. Nothing to do.
    pub async fn new() -> Self {
        WalRescaler {}
    }

    /// Update moving averages.
    fn update_moving_averages(
        &self,
        current_stats: &mut WalScalingInfo,
        metrics: Vec<Vec<u8>>,
        since: f64,
    ) {
        let mut user_activity = 0.0;
        for metric in &metrics {
            let metric: WalMetric = bincode::deserialize(metric).unwrap();
            for mem in metric.user_mems {
                user_activity += (mem as f64 / 1024.0) * REPLICATION_GAIN_SECS;
            }
        }
        // Divide by duration.
        user_activity /= since;
        // Update activity.
        current_stats.user_activity_gbsec = (1.0 - MOVING_FACTOR)
            * current_stats.user_activity_gbsec
            + MOVING_FACTOR * user_activity;
    }

    fn summarize_stats(&self, current_stats: &WalScalingInfo) -> (f64, f64) {
        // Compute price of two replicas.
        let ecs_price = (0.01234398 * REPLICAS_VCPUS) + (0.00135546 * REPLICAS_MEM_GB);
        let replicas_price = 2.0 * ecs_price;
        // Compute user gains.
        let lambda_gbsec_price = 0.0000166667;
        let user_cost = current_stats.user_activity_gbsec * lambda_gbsec_price * 3600.0;
        (replicas_price, user_cost)
    }
}
