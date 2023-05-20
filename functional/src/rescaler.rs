use common::adaptation::deploy::DeploymentInfo;
use common::adaptation::{Rescaler, ServerfulScalingState};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.25;
/// Extra compute to allocate despite cost savings.
/// It is possible to allocate up to 5-7x more compute than necessary without even increasing cost.
const EXTRA_COMPUTE: f64 = 1.2;
/// Maximum number of services per function.
/// Increase if necessary.
const MAXIMUM_FUNCTION_SCALE: u64 = 20;
/// Overhead of lambda.
const MAX_LAMBDA_OVERHEAD: f64 = 0.020;
/// Safe scale at which internal concurrency can be relied on (AZ+1 failure).
const SAFE_SCALE: f64 = 3.0;
/// Seek greater performance within this percentage of the optimal cost.
/// Set to 0 for cost-only optimization.
const PERF_OPTIMIZATION: f64 = 0.0;

/// Info to maintain for scaling functions.
#[derive(Serialize, Deserialize)]
struct FunctionalScalingInfo {
    activity: f64,
    waiting: f64,
}

/// Metrics for scaling functions.
#[derive(Serialize, Deserialize)]
pub(crate) struct FunctionalMetric {
    pub(crate) duration: std::time::Duration,
}

/// Rescaler for functions.
pub struct FunctionalRescaler {}

impl FunctionalRescaler {
    /// Initialize. Nothing to do.
    pub async fn new() -> Self {
        FunctionalRescaler {}
    }
}

#[async_trait::async_trait]
impl Rescaler for FunctionalRescaler {
    /// Just compute a moving average.
    async fn rescale(
        &self,
        scaling_state: &ServerfulScalingState,
        curr_timestamp: chrono::DateTime<chrono::Utc>,
        metrics: Vec<Value>,
    ) -> (u64, Value) {
        // Read deployment.
        let deployed_fn: DeploymentInfo =
            serde_json::from_value(scaling_state.deployment.clone()).unwrap();
        let deployed_fn = deployed_fn.fn_info.unwrap();
        // Read current activity from scaling info.
        let (mut activity, mut waiting) = if let Some(scaling_info) = &scaling_state.scaling_info {
            let scaling_info: FunctionalScalingInfo =
                serde_json::from_value(scaling_info.clone()).unwrap();
            (scaling_info.activity, scaling_info.waiting)
        } else {
            (0.0, 0.0)
        };
        // Compute the total activity of the new metrics.
        let total_interval = curr_timestamp
            .signed_duration_since(scaling_state.last_rescale)
            .num_seconds() as f64;
        let mut total_active_secs: f64 = 0.0;
        let mut total_waiting_secs: f64 = 0.0;
        println!("Num metrics: {}", metrics.len());
        for m in metrics.iter() {
            let m: FunctionalMetric = serde_json::from_value(m.clone()).unwrap();
            total_active_secs += m.duration.as_secs_f64();
            total_waiting_secs += MAX_LAMBDA_OVERHEAD;
        }
        // Compute moving averages.
        let new_activity = total_active_secs / total_interval;
        let new_waiting = total_waiting_secs / total_interval;
        activity = (1.0 - MOVING_FACTOR) * activity + MOVING_FACTOR * new_activity;
        waiting = (1.0 - MOVING_FACTOR) * waiting + MOVING_FACTOR * new_waiting;
        // Compute target scale.
        let ecs_cost: f64 = ((deployed_fn.total_ecs_cpus as f64) / 1024.0) * 0.012144
            + ((deployed_fn.total_ecs_mem as f64) / 1024.0) * 0.0013335;
        let lambda_cost: f64 =
            ((deployed_fn.function_mem as f64) / 1024.0) * (0.0000166667 * 3600.0) * activity;
        let waiting_cost: f64 =
            ((deployed_fn.caller_mem as f64) / 1024.0) * (0.0000166667 * 3600.0) * waiting;
        println!("Costs. lambda={lambda_cost}; ecs={ecs_cost}; Activity={activity}");
        let mut target_scale: f64 = (lambda_cost + waiting_cost) / ecs_cost;
        println!("Target Scale: {target_scale}");
        // Avoid allocated more than extra despite cost savings.
        if target_scale.floor() > activity * EXTRA_COMPUTE {
            target_scale = (activity * EXTRA_COMPUTE).ceil();
        }
        println!("Target Scale: {target_scale}");
        // After enough nodes are deployed, internal concurrency can safely handle requests.
        if target_scale > SAFE_SCALE {
            target_scale = SAFE_SCALE
                + (target_scale - SAFE_SCALE)
                    / ((deployed_fn.concurrency * deployed_fn.num_workers) as f64);
        }
        if PERF_OPTIMIZATION > 1e-4 {
            target_scale = target_scale + PERF_OPTIMIZATION * target_scale;
        }
        // Set new scale.
        let mut new_scale = target_scale.floor() as u64;
        if new_scale > MAXIMUM_FUNCTION_SCALE {
            new_scale = MAXIMUM_FUNCTION_SCALE;
        }
        println!("New Scale: {target_scale}");
        let new_scaling_info = FunctionalScalingInfo { activity, waiting };
        (new_scale, serde_json::to_value(&new_scaling_info).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::{FunctionalMetric, FunctionalRescaler, FunctionalScalingInfo, MOVING_FACTOR};
    use common::adaptation::frontend::AdapterFrontend;
    use common::adaptation::{AdapterScaling, ScalerReq};
    use common::leasing::Leaser;
    use common::{full_scaling_queue_name, scaling_table_name};
    use serde_json::Value;
    use std::sync::Arc;

    struct TestObject {
        test_start_time: chrono::DateTime<chrono::Utc>,
        scaling: AdapterScaling,
        front_end: AdapterFrontend,
        offset: u32,
    }

    impl TestObject {
        /// Create test.
        async fn new() -> TestObject {
            let subsystem = "functional";
            let namespace = "functional";
            let name = "sleeper";
            let rescaler = Arc::new(FunctionalRescaler::new().await);
            let scaling = AdapterScaling::new(Some(rescaler), subsystem).await;
            let front_end = AdapterFrontend::new(subsystem, namespace, name).await;
            let t = TestObject {
                test_start_time: chrono::Utc::now(),
                scaling,
                front_end,
                offset: 0,
            };
            t.scaling.reset_revision(&t.front_end.info.namespace).await;
            // Delete lease held by previous tests.
            let scaling_table = scaling_table_name(subsystem);
            let queue_name = full_scaling_queue_name(subsystem, namespace, name);
            let leaser = Leaser::new(&scaling_table).await;
            leaser.delete(&queue_name).await;
            t
        }

        /// Populate dummy metrics for sleeper function.
        async fn populate_sleeper_metrics(&mut self, activity: f64) {
            let mut metrics: Vec<Value> = Vec::new();
            let function_duration = 2; // One function duration.
            let simulation_interval: u32 = 60; // Simulation duration.
            let num_entries = activity * ((simulation_interval / function_duration) as f64);
            let num_entries = num_entries.round() as i32;
            for _ in 0..num_entries {
                let m = FunctionalMetric {
                    duration: std::time::Duration::from_secs(function_duration as u64),
                };
                metrics.push(serde_json::to_value(&m).unwrap());
            }
            let timestamp = self
                .test_start_time
                .checked_add_signed(chrono::Duration::seconds(self.offset as i64))
                .unwrap();
            self.offset += simulation_interval;
            let req = ScalerReq {
                namespace: self.front_end.info.namespace.clone(),
                name: self.front_end.info.name.clone(),
                timestamp,
            };
            // Send in batches to test multiple reads.
            let mut lo = 0;
            while lo < metrics.len() {
                let hi = if lo + 5 < metrics.len() {
                    lo + 5
                } else {
                    metrics.len()
                };
                self.front_end.push_metrics(&metrics[lo..hi]).await;
                lo = hi;
            }
            self.scaling.handle_request(&req, false).await;
        }

        /// Check that activity is as expected.
        async fn check_activity(&self, expected_activity: f64, expected_scaled: Option<bool>) {
            let timestamp = self
                .test_start_time
                .checked_add_signed(chrono::Duration::seconds(self.offset as i64))
                .unwrap();

            let req = ScalerReq {
                namespace: self.front_end.info.namespace.clone(),
                name: self.front_end.info.name.clone(),
                timestamp,
            };
            let scaling_state = self.scaling.handle_request(&req, false).await;
            println!("Scaling State: {scaling_state:?}");
            let scaling_info = if let Some(scaling_info) = scaling_state.scaling_info {
                serde_json::from_value(scaling_info).unwrap()
            } else {
                FunctionalScalingInfo {
                    activity: 0.0,
                    waiting: 0.0,
                }
            };
            let activity_lo = expected_activity - expected_activity / 10.0;
            let activity_hi = expected_activity + expected_activity / 10.0;
            println!("Checking: exp={expected_activity}; lo={activity_lo}; hi={activity_hi};");
            assert!(scaling_info.activity >= activity_lo);
            assert!(scaling_info.activity <= activity_hi);
            if let Some(expected_scaled) = expected_scaled {
                assert!(!(expected_scaled && scaling_state.current_scale == 0));
            }
        }
    }

    /// #[tokio::test] prevents linting. So I make this a separate function.
    async fn simple_test() {
        let mut t = TestObject::new().await;
        // Initial scale should be 0.
        let ma = 0.0;
        t.check_activity(0.0, Some(false)).await;
        // Set to 0.5
        let activity = 0.5;
        let ma = (1.0 - MOVING_FACTOR) * ma + MOVING_FACTOR * activity;
        t.populate_sleeper_metrics(activity).await;
        t.check_activity(ma, Some(false)).await;
        // Keep at 0.5
        let activity = 0.5;
        let ma = (1.0 - MOVING_FACTOR) * ma + MOVING_FACTOR * activity;
        t.populate_sleeper_metrics(activity).await;
        t.check_activity(ma, Some(false)).await;
        // Set to 10
        let activity = 10.0;
        let ma = (1.0 - MOVING_FACTOR) * ma + MOVING_FACTOR * activity;
        t.populate_sleeper_metrics(activity).await;
        t.check_activity(ma, None).await;
        // Keep at 10.
        let activity = 10.0;
        let ma = (1.0 - MOVING_FACTOR) * ma + MOVING_FACTOR * activity;
        t.populate_sleeper_metrics(activity).await;
        t.check_activity(ma, Some(true)).await;
        // Set to 0.5
        let activity = 0.5;
        let ma = (1.0 - MOVING_FACTOR) * ma + MOVING_FACTOR * activity;
        t.populate_sleeper_metrics(activity).await;
        t.check_activity(ma, Some(true)).await;
    }

    #[tokio::test]
    async fn test_rescaling() {
        simple_test().await;
    }
}
