use std::collections::{BTreeMap, HashMap};

use common::{HandlerScalingState, Rescaler, RescalingResult, SubsystemScalingState};
use serde::{Deserialize, Serialize};

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.25;
/// Overhead of lambda.
const AVG_LAMBDA_OVERHEAD_SECS: f64 = 0.013;
/// Upscaling for stability. When optimal deployment is too close to two configs,
/// Oscillating between them leads to much lower performance.
const UPSCALE_FACTOR: f64 = 1.25;
/// Forecasting granularity.
pub const FORECASTING_GRANULARITY_SECS: f64 = 10.0;
/// Scale down delay.
pub const SCALE_DOWN_DELAY_SECS: i64 = 60;

/// Exploration stats.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExplorationStats {
    num_points: f64,
    avg_latency: f64,
}

/// Info to maintain for scaling functions.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionalScalingInfo {
    invocation_rate: f64,
    global_avg_latency: f64,
    caller_mem_avg: f64,
    lambda_stats: ExplorationStats,
    exploration_stats: BTreeMap<i32, ExplorationStats>,
    scheduled_scale_downs: BTreeMap<i32, chrono::DateTime<chrono::Utc>>,
    // Forecast of invocation rates.
    // The datetime is the start of the forecast interval.
    // vec[i] should contain invocatio rate atstart_time + 30s.
    forecasted_invocations: Option<(chrono::DateTime<chrono::Utc>, Vec<f64>)>,
}

/// Metrics for scaling functions.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct FunctionalMetric {
    pub(crate) duration: std::time::Duration,
    pub(crate) mem_size_mb: i32,
    pub(crate) mem_usage: Option<f64>,
    pub(crate) cpu_usage: Option<f64>,
    pub(crate) caller_mem: i32,
}

/// Rescaler for functions.
pub struct FunctionalRescaler {}

#[async_trait::async_trait]
impl Rescaler for FunctionalRescaler {
    /// Perform rescaling.
    async fn rescale(
        &self,
        subsystem_scaling_state: &SubsystemScalingState,
        handler_scaling_state: Option<&HandlerScalingState>,
        since_last_rescale: std::time::Duration,
        metrics: Vec<Vec<u8>>,
    ) -> RescalingResult {
        // Get handler state.
        let handler_scaling_state = handler_scaling_state.unwrap();
        // Make default rescaling result.
        let mut rescaling_result = RescalingResult {
            services_scales: subsystem_scaling_state.service_scales.clone(),
            services_scaling_info: subsystem_scaling_state.scaling_info.clone(),
            handler_scales: Some(handler_scaling_state.handler_scales.clone()),
            handler_scaling_info: handler_scaling_state.scaling_info.clone(),
        };
        // Read current stats or make new ones.
        let mut current_stats = handler_scaling_state.scaling_info.as_ref().map_or(
            FunctionalScalingInfo {
                global_avg_latency: 0.001,
                invocation_rate: 0.0,
                caller_mem_avg: 0.0,
                lambda_stats: ExplorationStats {
                    num_points: 0.0,
                    avg_latency: 0.001, // 1ms default.
                },
                scheduled_scale_downs: BTreeMap::new(),
                exploration_stats: handler_scaling_state
                    .handler_scales
                    .keys()
                    .map(|m| {
                        (
                            *m,
                            ExplorationStats {
                                num_points: 0.0,
                                avg_latency: 0.001, // 1ms default.
                            },
                        )
                    })
                    .collect(),
                forecasted_invocations: None,
            },
            |s| serde_json::from_str(s).unwrap(),
        );
        // Update moving averages.
        self.update_moving_averages(
            &mut current_stats,
            &handler_scaling_state,
            metrics,
            since_last_rescale.as_secs_f64(),
        );
        self.try_use_forcasted_invocations(&mut current_stats);
        // Find ideal mem and summary.
        let to_explore = self.get_mems_to_explore(
            &current_stats,
            &subsystem_scaling_state,
            handler_scaling_state,
        );
        println!("To Explore: {to_explore:?}");
        let mut ideal_mem =
            self.find_ideal_memory(&mut current_stats, &handler_scaling_state, &to_explore);
        let min_mem = *current_stats.exploration_stats.keys().min().unwrap();
        while ideal_mem > min_mem {
            let (
                lambda_price,
                ecs_compute_price,
                invoker_price,
                ecs_user_cost,
                _observed_concurrency,
            ) = self.summarize_stats(
                &current_stats,
                &subsystem_scaling_state,
                &handler_scaling_state,
                ideal_mem,
            );
            // If price is ok, break.
            if ecs_compute_price + ecs_user_cost + invoker_price <= lambda_price {
                break;
            }
            // If price is too high, try next mem.
            ideal_mem /= 2;
        }
        // Resummarize using new ideal mem in case it changed.
        let (lambda_price, ecs_compute_price, invoker_price, ecs_user_cost, observed_concurrency) =
            self.summarize_stats(
                &current_stats,
                &subsystem_scaling_state,
                &handler_scaling_state,
                ideal_mem,
            );
        // If base price still too high shutdown everything.
        if ecs_compute_price + ecs_user_cost + invoker_price > lambda_price {
            self.schedule_shutdowns(
                &mut current_stats,
                &handler_scaling_state,
                &mut rescaling_result,
                None,
                true,
            );
        } else {
            // If base price enough, shutdown everything except ideal instance type.
            self.schedule_shutdowns(
                &mut current_stats,
                &handler_scaling_state,
                &mut rescaling_result,
                Some(ideal_mem),
                false,
            );
        }
        // Determines the number of base containers we can deploy in a cost effective manner and still be able to handle the load.
        let cost_ratio = (lambda_price - invoker_price - ecs_user_cost) / ecs_compute_price;
        let target_scale =
            observed_concurrency / handler_scaling_state.handler_spec.concurrency as f64;
        let mut target_scale = target_scale;
        if target_scale > 2.0 {
            // Hack to prevent oscillations around ideal concurency.
            // At this point, ECS is so much cheaper than Lambda that extra deployments are ok.
            target_scale = target_scale * UPSCALE_FACTOR;
        }
        let to_deploy: f64 = if cost_ratio.floor() > target_scale.ceil() {
            target_scale.ceil()
        } else {
            cost_ratio.floor()
        };
        let to_deploy = to_deploy as i32;
        self.mark_rescaling(
            &mut rescaling_result,
            &handler_scaling_state,
            ideal_mem,
            to_deploy,
        );
        println!("Summary: LP={lambda_price}, EP={ecs_compute_price}, IP={invoker_price}, OC={observed_concurrency}, CR={cost_ratio}, TS={target_scale}, TD={to_deploy}, IM={ideal_mem}");
        rescaling_result.handler_scaling_info =
            Some(serde_json::to_string(&current_stats).unwrap());
        rescaling_result
    }
}

impl FunctionalRescaler {
    /// Initialize. Nothing to do.
    pub async fn new() -> Self {
        FunctionalRescaler {}
    }

    /// Update moving averages.
    fn update_moving_averages(
        &self,
        current_stats: &mut FunctionalScalingInfo,
        _handler_scaling_state: &HandlerScalingState,
        metrics: Vec<Vec<u8>>,
        since: f64,
    ) {
        let mut num_invocations = 0.0;
        let mut lambda_exploration = ExplorationStats {
            num_points: 0.0,
            avg_latency: 0.0,
        };
        let mut ecs_explorations = HashMap::<i32, ExplorationStats>::new();
        let mut caller_mem = 0.0;
        let mut global_avg_latency = 0.0;
        for metric in &metrics {
            // Try reading forecast.
            if let Ok(forecast) =
                bincode::deserialize::<(chrono::DateTime<chrono::Utc>, Vec<f64>)>(metric)
            {
                current_stats.forecasted_invocations = Some(forecast);
                continue;
            }
            let metric: FunctionalMetric = bincode::deserialize(metric).unwrap();
            let duration_secs = metric.duration.as_secs_f64();
            // Update stats.
            num_invocations += 1.0;
            caller_mem += metric.caller_mem as f64;
            global_avg_latency += duration_secs;
            // Update exploration stats.
            let exploration = if metric.mem_usage.is_none() {
                // Lambda.
                &mut lambda_exploration
            } else {
                // ECS.
                if !ecs_explorations.contains_key(&metric.mem_size_mb) {
                    ecs_explorations.insert(
                        metric.mem_size_mb,
                        ExplorationStats {
                            num_points: 0.0,
                            avg_latency: 0.0,
                        },
                    );
                }
                ecs_explorations.get_mut(&metric.mem_size_mb).unwrap()
            };
            exploration.avg_latency += duration_secs;
            exploration.num_points += 1.0;
        }
        // Take average and divide by duration.
        if num_invocations > 0.0 {
            caller_mem /= num_invocations;
            global_avg_latency /= num_invocations;
        }
        num_invocations /= since;
        println!("Invocations/second: {num_invocations:?}");
        // Update global stats.
        current_stats.invocation_rate =
            (1.0 - MOVING_FACTOR) * current_stats.invocation_rate + MOVING_FACTOR * num_invocations;
        if current_stats.global_avg_latency == 0.0 {
            // First time.
            current_stats.global_avg_latency = global_avg_latency;
            current_stats.caller_mem_avg = caller_mem;
        } else {
            // Moving average.
            current_stats.global_avg_latency = (1.0 - MOVING_FACTOR)
                * current_stats.global_avg_latency
                + MOVING_FACTOR * global_avg_latency;
            current_stats.caller_mem_avg =
                (1.0 - MOVING_FACTOR) * current_stats.caller_mem_avg + MOVING_FACTOR * caller_mem;
        }
        // Update lambda stats.
        if lambda_exploration.num_points > 0.0 {
            lambda_exploration.avg_latency /= lambda_exploration.num_points;
            if current_stats.lambda_stats.num_points == 0.0 {
                // First time.
                current_stats.lambda_stats.avg_latency = lambda_exploration.avg_latency;
            } else {
                // Moving average.
                current_stats.lambda_stats.avg_latency = (1.0 - MOVING_FACTOR)
                    * current_stats.lambda_stats.avg_latency
                    + MOVING_FACTOR * lambda_exploration.avg_latency;
            }
            current_stats.lambda_stats.num_points += 1.0;
        }
        // Update ecs stats.
        for (mem, exploration) in &mut current_stats.exploration_stats {
            if let Some(new_exploration) = ecs_explorations.get_mut(&mem) {
                new_exploration.avg_latency /= new_exploration.num_points;
                if exploration.num_points == 0.0 {
                    // First time.
                    exploration.avg_latency = new_exploration.avg_latency;
                } else {
                    // Moving average.
                    exploration.avg_latency = (1.0 - MOVING_FACTOR) * exploration.avg_latency
                        + MOVING_FACTOR * new_exploration.avg_latency;
                }
                exploration.num_points += 1.0;
            }
        }
    }

    fn get_mems_to_explore(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
    ) -> Vec<i32> {
        // Prevent pointless oscillations under low usage (defined by magic constants).
        let global_concurrency: f64 =
            current_stats.global_avg_latency * current_stats.invocation_rate;
        let min_mem = *current_stats.exploration_stats.keys().min().unwrap();
        let (stable_usage, invoker_coef) = if handler_scaling_state.handler_spec.unique {
            (true, 0.0)
        } else {
            (global_concurrency > 0.15, 1.0)
        };
        println!("get_mems_to_explore. global_concurrency={global_concurrency}.");
        let mut res = Vec::new();
        let mut min_instance_cost: Option<f64> = None;
        for (mem, _exploration) in &current_stats.exploration_stats {
            // Compute expected target scale.
            let observed_latency =
                self.get_instance_latency(current_stats, handler_scaling_state, *mem);
            let expected_concurreny = observed_latency * current_stats.invocation_rate;
            let target_scale = expected_concurreny.ceil();
            // Compute costs.
            let (ecs_price, invoker_price, user_cost) =
                self.get_instance_cost(&current_stats, subsys_state, handler_scaling_state, *mem);
            // Total cost.
            let total_cost = user_cost + target_scale * ecs_price + invoker_coef * invoker_price;
            println!("Mem {mem}. Total Cost={total_cost}. User Cost={user_cost}. ECS={ecs_price}. TS={target_scale}. OL={observed_latency}.");
            if min_instance_cost.is_none() {
                min_instance_cost = Some(total_cost);
            }
            let max_allowable_cost =
                (1.0 + handler_scaling_state.handler_spec.scaleup) * min_instance_cost.unwrap();
            if *mem == min_mem {
                // Always push min mem.
                res.push(*mem);
            } else if total_cost <= max_allowable_cost && stable_usage {
                // Push greater mem if cost within bounds and stable usage.
                res.push(*mem);
            }
        }
        res
    }

    /// Return the maximum allowable instance.
    fn find_ideal_memory(
        &self,
        _current_stats: &mut FunctionalScalingInfo,
        _handler_scaling_state: &HandlerScalingState,
        to_explore: &[i32],
    ) -> i32 {
        // Assumes sorted order.
        return *to_explore.last().unwrap();
    }

    /// Schedule the right shutdowns.
    fn schedule_shutdowns(
        &self,
        current_stats: &mut FunctionalScalingInfo,
        handler_scaling_state: &HandlerScalingState,
        rescaling_result: &mut RescalingResult,
        ideal_mem: Option<i32>,
        everything: bool,
    ) {
        // Schedule scale downs.
        let handler_scales = rescaling_result.handler_scales.as_mut().unwrap();
        let now = chrono::Utc::now();
        for (mem, scale) in &handler_scaling_state.handler_scales {
            // Check if already shutdown.
            if *scale == 0 {
                current_stats.scheduled_scale_downs.remove(mem);
                continue;
            }
            // Check if current instance type (should not be shutdown).
            if ideal_mem == Some(*mem) {
                current_stats.scheduled_scale_downs.remove(mem);
                continue;
            }
            // If not yet scheduled, do so.
            if !current_stats.scheduled_scale_downs.contains_key(mem) {
                println!("Scheduling for shutdown: {mem}");
                current_stats
                    .scheduled_scale_downs
                    .insert(*mem, now + chrono::Duration::seconds(SCALE_DOWN_DELAY_SECS));
            }
            // If already scheduled, check if should shutdown.
            // When scaling down to 0, just shutdown everything now.
            let scale_down_time = current_stats.scheduled_scale_downs.get(mem).unwrap();
            if *scale_down_time < now || everything {
                println!("Actually shutting down: {mem}");
                handler_scales.insert(*mem, 0);
            }
        }
    }

    /// Get latency.
    fn get_instance_latency(
        &self,
        current_stats: &FunctionalScalingInfo,
        handler_scaling_state: &HandlerScalingState,
        mem: i32,
    ) -> f64 {
        let ecs_vcpu = (mem / 2) as f64 / 1024.0;
        let lambda_vcpu = handler_scaling_state.handler_spec.default_mem as f64 / 1769.0;
        if let Some(exploration) = current_stats.exploration_stats.get(&mem) {
            if exploration.num_points >= (1.0 / MOVING_FACTOR) {
                // Enough points to trust.
                exploration.avg_latency
            } else {
                // Find the previous instance type with enough points.
                let mut prev_latency_vcpu = None;
                for (other_mem, other_exploration) in &current_stats.exploration_stats {
                    if *other_mem < mem && other_exploration.num_points >= (1.0 / MOVING_FACTOR) {
                        let prev_latency = other_exploration.avg_latency;
                        let prev_vcpu = (*other_mem / 2) as f64 / 1024.0;
                        prev_latency_vcpu = Some((prev_latency, prev_vcpu));
                    }
                }
                let (prev_latency, prev_vcpu) = if let Some(x) = prev_latency_vcpu {
                    x
                } else {
                    let prev_latency = current_stats.lambda_stats.avg_latency;
                    (prev_latency, lambda_vcpu)
                };
                // Assume semi-linear scaling when not enough points.
                // This balances between perfect scaling and no scaling.
                let mut scaling = 1.0 + ((ecs_vcpu - prev_vcpu) / prev_vcpu) / 2.0;
                if scaling > 2.0 {
                    scaling = 2.0;
                }
                prev_latency / scaling
            }
        } else {
            current_stats.lambda_stats.avg_latency
        }
    }

    /// Get instance cost.
    /// Returns (ecs_compute_price, invoker_price, user_cost).
    fn get_instance_cost(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
        mem: i32,
    ) -> (f64, f64, f64) {
        // Compute ecs cost.
        let cpus = mem / 2;
        let ecs_mem_gb = mem as f64 / 1024.0;
        let ecs_cpus = cpus as f64 / 1024.0;
        let ecs_compute_price = if handler_scaling_state.handler_spec.spot {
            (0.01234398 * ecs_cpus) + (0.00135546 * ecs_mem_gb)
        } else {
            (0.04048 * ecs_cpus) + (0.004445 * ecs_mem_gb)
        };
        println!(
            "Instance price {mem}: {ecs_compute_price}. Spot={}!",
            handler_scaling_state.handler_spec.spot
        );
        // Invoker is only needed for non-unique functions (non-actors).
        let invoker_price = if handler_scaling_state.handler_spec.unique {
            0.0
        } else {
            let invoker_specs = subsys_state.service_specs.get("invoker").unwrap();
            let invoker_mem = invoker_specs.mem as f64 / 1024.0;
            let invoker_cpus = invoker_specs.cpus as f64 / 1024.0;
            (0.01234398 * invoker_cpus) + (0.00135546 * invoker_mem)
        };
        // Compute user cost.
        let caller_mem_gb = current_stats.caller_mem_avg / 1024.0;
        let avg_latency = self.get_instance_latency(current_stats, handler_scaling_state, mem);
        let user_activity = current_stats.invocation_rate * avg_latency;
        let user_activity_gbsec = user_activity * caller_mem_gb;
        let user_cost = user_activity_gbsec * 0.0000166667 * 3600.0;
        return (ecs_compute_price, invoker_price, user_cost);
    }

    /// Summarize current statistics.
    /// Returns (lambda_price, ecs_compute_price, invoker_price, ecs_user_cost, observed_concurrency).
    fn summarize_stats(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
        ideal_mem: i32,
    ) -> (f64, f64, f64, f64, f64) {
        // Compute lambda cost.
        let default_mem_gb = handler_scaling_state.handler_spec.default_mem as f64 / 1024.0;
        let caller_mem_gb = current_stats.caller_mem_avg / 1024.0;
        let fn_call_cost = current_stats.invocation_rate * 0.2 * 3600.0 / 1e6;
        let fn_activity = current_stats.invocation_rate * current_stats.lambda_stats.avg_latency;
        let fn_activity_gbsec = fn_activity * default_mem_gb;
        let fn_compute_cost = fn_activity_gbsec * 0.0000166667 * 3600.0;
        let fn_user_activity = current_stats.invocation_rate
            * (current_stats.lambda_stats.avg_latency + AVG_LAMBDA_OVERHEAD_SECS);
        let fn_user_activity_gbsec = fn_user_activity * caller_mem_gb;
        let fn_user_cost = fn_user_activity_gbsec * 0.0000166667 * 3600.0;
        let lambda_price = fn_user_cost + fn_compute_cost + fn_call_cost;
        // Compute observed concurrency.
        let observed_concurrency: f64 =
            current_stats.global_avg_latency * current_stats.invocation_rate;
        // Compute ecs cost.
        let (ecs_compute_price, ecs_invoker_price, ecs_user_cost) = self.get_instance_cost(
            current_stats,
            subsys_state,
            handler_scaling_state,
            ideal_mem,
        );
        return (
            lambda_price,
            ecs_compute_price,
            ecs_invoker_price,
            ecs_user_cost,
            observed_concurrency,
        );
    }

    /// Mark what to rescale.
    fn mark_rescaling(
        &self,
        rescaling_result: &mut RescalingResult,
        handler_scaling_state: &HandlerScalingState,
        ideal_mem: i32,
        mut to_deploy: i32,
    ) {
        if to_deploy <= 0 {
            rescaling_result.services_scales.insert("invoker".into(), 0);
            return;
        }
        // Hack to disallow scaling for benchmarks against a lambda baseline.
        if handler_scaling_state.handler_spec.scaleup < -(1e-4) {
            return;
        }

        // Special case for actors.
        if handler_scaling_state.handler_spec.unique {
            rescaling_result.services_scales.insert("invoker".into(), 0);
            to_deploy = 1;
        } else {
            println!("Setting service scales");
            rescaling_result.services_scales.insert("invoker".into(), 1);
            let old_to_deploy = rescaling_result.handler_scales.as_ref().unwrap();
            let old_to_deploy = old_to_deploy.get(&ideal_mem).cloned().unwrap_or(0) as i32;
            // To guarantee stability around ceil, only go down by at least two.
            if to_deploy > 2 && (to_deploy + 1) == old_to_deploy {
                to_deploy = old_to_deploy;
            }
        };
        // Update scales.
        let handler_scales = rescaling_result.handler_scales.as_mut().unwrap();
        handler_scales.insert(ideal_mem, to_deploy as u64);
    }

    /// Try using forecasted invocations.
    fn try_use_forcasted_invocations(&self, current_stats: &mut FunctionalScalingInfo) {
        if let Some((forecast_start, forecasts)) = &current_stats.forecasted_invocations {
            // Find index of current time.
            let now = chrono::Utc::now();
            let since = now.signed_duration_since(*forecast_start).num_seconds() as f64;
            let idx = (since / FORECASTING_GRANULARITY_SECS) as usize;
            // Compute forecast window and lookahead.
            // Window should be ~30s (beneficial soaking of burst).
            // Lookahead should be ~60s (pessimistic startup time).
            let forecast_window = 1 + (30.0 / FORECASTING_GRANULARITY_SECS) as usize;
            let forecast_lookahead = (60.0 / FORECASTING_GRANULARITY_SECS) as usize;
            let lookahead_start = idx + forecast_lookahead;
            let lookahead_end = lookahead_start + forecast_window;
            if lookahead_end > forecasts.len() {
                current_stats.forecasted_invocations = None;
                return;
            }
            // Take average invocation rate in the lookahead window.
            let next_window = &forecasts[lookahead_start..lookahead_end];
            let forecasted_invocation_rate: f64 =
                next_window.iter().sum::<f64>() / next_window.len() as f64;
            // Now find the max forecasted in the current window.
            let start_idx = if idx > 0 { idx - 1 } else { idx };
            let curr_window = &forecasts[(start_idx)..(idx + forecast_window)];
            let max_invocation_rate = curr_window
                .iter()
                .max_by(|x, y| x.partial_cmp(y).unwrap())
                .unwrap();
            // Set the invocation rate, being careful not to prematurely decrease.
            if forecasted_invocation_rate > current_stats.invocation_rate {
                // It's always ok to increase before hand.
                println!(
                    "Using forecasted invocation rate (increase): {forecasted_invocation_rate}."
                );
                current_stats.invocation_rate = forecasted_invocation_rate;
            } else {
                // Avoid decreasing below the current window.
                if *max_invocation_rate < forecasted_invocation_rate {
                    println!("Using forecasted invocation rate (decrease): {forecasted_invocation_rate}.");
                    current_stats.invocation_rate = forecasted_invocation_rate;
                } else {
                    println!("Avoiding forecast decrease. Max={max_invocation_rate}. Forecasted={forecasted_invocation_rate}.");
                    current_stats.invocation_rate = *max_invocation_rate;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common::deployment::{container, HandlerSpec, ServiceSpec};
    use common::Rescaler;

    use crate::rescaler::FunctionalScalingInfo;

    use super::{FunctionalMetric, FunctionalRescaler, FORECASTING_GRANULARITY_SECS};
    use super::{HandlerScalingState, SubsystemScalingState};

    const HANDLER_MEM: i32 = 1769;
    const MIN_ECS_MEM: i32 = 2048;

    fn test_handler_state(scaleup: f64) -> HandlerScalingState {
        let mems = container::ContainerDeployment::all_avail_mems(HANDLER_MEM, scaleup);
        HandlerScalingState {
            subsystem: "functional".into(),
            namespace: "functional".into(),
            identifier: "echofn".into(),
            peers: HashMap::new(),
            scaling_info: None,
            handler_spec: HandlerSpec {
                subsystem: "functional".into(),
                namespace: "functional".into(),
                name: "echofn".into(),
                timeout: 10,
                default_mem: HANDLER_MEM,
                concurrency: 1,
                ephemeral: 512,
                persistent: false,
                unique: false,
                scaleup,
                spot: true,
            },
            handler_scales: mems.into_iter().map(|m| (m, 0)).collect(),
        }
    }

    fn test_subsys_state() -> SubsystemScalingState {
        let peers = vec![(String::from("invoker"), HashMap::new())];
        let scales = vec![(String::from("invoker"), 0)];
        let specs = vec![(
            String::from("invoker"),
            ServiceSpec {
                namespace: "functional".into(),
                name: "echofn".into(),
                timeout: 1,
                mem: 512,
                cpus: 256,
                unique: false,
            },
        )];
        SubsystemScalingState {
            subsystem: "functional".into(),
            namespace: "functional".into(),
            identifier: "echofn".into(),
            peers: peers.into_iter().collect(),
            service_scales: scales.into_iter().collect(),
            scaling_info: None,
            service_specs: specs.into_iter().collect(),
        }
    }

    fn test_metrics(num_metrics: usize, mem_size_mb: i32, for_lambda: bool) -> Vec<Vec<u8>> {
        let mut metrics = Vec::new();
        let util = if for_lambda { None } else { Some(0.5) };
        for _ in 0..num_metrics {
            let metric = FunctionalMetric {
                duration: std::time::Duration::from_secs(1),
                mem_size_mb,
                mem_usage: util,
                cpu_usage: util,
                caller_mem: 1, // Negligible. Makes testing easier.
            };
            let metric = bincode::serialize(&metric).unwrap();
            metrics.push(metric);
        }
        metrics
    }

    async fn rescale_with_metrics(
        fn_rescaler: &FunctionalRescaler,
        handler_state: &mut HandlerScalingState,
        subsys_state: &mut SubsystemScalingState,
        metrics: Vec<Vec<u8>>,
    ) {
        rescale_with_metrics_and_forecast(fn_rescaler, handler_state, subsys_state, metrics, None)
            .await;
    }

    async fn rescale_with_metrics_and_forecast(
        fn_rescaler: &FunctionalRescaler,
        handler_state: &mut HandlerScalingState,
        subsys_state: &mut SubsystemScalingState,
        mut metrics: Vec<Vec<u8>>,
        forecast: Option<(chrono::DateTime<chrono::Utc>, Vec<f64>)>,
    ) {
        if let Some(forecast) = forecast {
            let forecast = bincode::serialize(&forecast).unwrap();
            metrics.push(forecast);
        }
        let since_last_rescaling = std::time::Duration::from_secs(10);
        let rescaling_result = fn_rescaler
            .rescale(
                &subsys_state,
                Some(&handler_state),
                since_last_rescaling,
                metrics,
            )
            .await;
        subsys_state.service_scales = rescaling_result.services_scales;
        subsys_state.scaling_info = rescaling_result.services_scaling_info;
        handler_state.handler_scales = rescaling_result.handler_scales.unwrap();
        handler_state.scaling_info = rescaling_result.handler_scaling_info;
    }

    fn show_handler_state(prompt: &str, handler_state: &HandlerScalingState) {
        let mut to_show = handler_state.clone();
        let scaling_info = to_show.scaling_info.clone();
        let scaling_info: Option<FunctionalScalingInfo> =
            scaling_info.map(|s| serde_json::from_str(&s).unwrap());
        to_show.scaling_info = None;

        println!("{prompt} {to_show:?}");
        println!("{prompt} {scaling_info:?}");
    }

    fn compute_actual_scales(handler_state: &HandlerScalingState) -> HashMap<i32, u64> {
        let scaling_info: Option<FunctionalScalingInfo> = handler_state
            .scaling_info
            .as_ref()
            .map(|s| serde_json::from_str(&s).unwrap());
        let mut scales = handler_state.handler_scales.clone();
        if let Some(scaling_info) = scaling_info {
            for (mem, _) in scaling_info.scheduled_scale_downs {
                scales.insert(mem, 0);
            }
        }
        scales
    }

    /// Increase and decrease rate of call to see if scaling happens correctly.
    #[tokio::test]
    async fn basic_rescaling_test() {
        run_basic_rescaling_test().await;
    }

    async fn run_basic_rescaling_test() {
        let fn_rescaler = FunctionalRescaler::new().await;
        let mut subsys_state = test_subsys_state();
        let mut handler_state = test_handler_state(0.0);
        let min_mem_mb = MIN_ECS_MEM;
        let init_lambda_metrics = test_metrics(10, min_mem_mb, true);
        rescale_with_metrics(
            &fn_rescaler,
            &mut handler_state,
            &mut subsys_state,
            init_lambda_metrics,
        )
        .await;
        for _ in 0..100 {
            let metrics = test_metrics(10, min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.invocation_rate > 0.9 && scaling_info.invocation_rate < 1.1);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        let handler_scales = compute_actual_scales(&handler_state);
        // assert!(handler_scales.get(&512).unwrap() == &1);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &1); // 1024 is minimum size.
                                                                 // Now underutilize. Should scale down.
        for _ in 0..100 {
            let metrics = vec![];
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After PostHandler:", &handler_state);
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.invocation_rate < 0.01);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &0);
        let handler_scales = compute_actual_scales(&handler_state);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &0);
        // Use concurrently. Should scale out.
        for _ in 0..100 {
            let metrics = test_metrics(20, min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        let handler_scales = compute_actual_scales(&handler_state);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &2);
    }

    /// Test if scale up happens correctly.
    #[tokio::test]
    async fn basic_scaleup_test() {
        run_basic_scaleup_test().await;
    }

    async fn run_basic_scaleup_test() {
        let fn_rescaler = FunctionalRescaler::new().await;
        let mut subsys_state = test_subsys_state();
        let mut handler_state = test_handler_state(1.0);
        let min_mem_mb = MIN_ECS_MEM;
        let scaled_mem_mb = 2 * min_mem_mb;
        let init_lambda_metrics = test_metrics(10, min_mem_mb, true);
        rescale_with_metrics(
            &fn_rescaler,
            &mut handler_state,
            &mut subsys_state,
            init_lambda_metrics,
        )
        .await;
        // First run without concurrency.
        for _ in 0..100 {
            let metrics = test_metrics(10, min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        // Before scheduled time, both instances should be active. After, only the larger one.
        let pre_handler_scales = handler_state.handler_scales.clone();
        let post_handler_scales = compute_actual_scales(&handler_state);
        assert!(pre_handler_scales.get(&min_mem_mb).unwrap() == &1);
        assert!(pre_handler_scales.get(&scaled_mem_mb).unwrap() == &1);
        assert!(post_handler_scales.get(&min_mem_mb).unwrap() == &0);
        assert!(post_handler_scales.get(&scaled_mem_mb).unwrap() == &1);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        // Now run with more concurrency.
        for _ in 0..100 {
            let metrics = test_metrics(20, 2 * min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        // Only The larger instance should scale out.
        let post_handler_scales = compute_actual_scales(&handler_state);
        assert!(post_handler_scales.get(&min_mem_mb).unwrap() == &0);
        assert!(post_handler_scales.get(&scaled_mem_mb).unwrap() > &1);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        // Now underutilize. Should all scale down.
        for _ in 0..100 {
            let metrics = vec![];
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        let post_handler_scales = compute_actual_scales(&handler_state);
        show_handler_state("After Handler:", &handler_state);
        assert!(post_handler_scales.get(&min_mem_mb).unwrap() == &0);
        assert!(post_handler_scales.get(&scaled_mem_mb).unwrap() == &0);
        println!("After Subsys: {subsys_state:?}");
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &0);
    }

    /// Test if forecasting is correct.
    #[tokio::test]
    async fn basic_forecast_test() {
        run_basic_forecast_test().await;
    }

    async fn run_basic_forecast_test() {
        let fn_rescaler = FunctionalRescaler::new().await;
        let mut subsys_state = test_subsys_state();
        let mut handler_state = test_handler_state(0.0);
        let min_mem_mb = MIN_ECS_MEM;
        let init_lambda_metrics = test_metrics(10, min_mem_mb, true);
        // Pass forecasting data.
        let now = chrono::Utc::now();
        // For 2 minutes, forecast 100 invocations per second.
        let num_points = (2.0 * 60.0 / FORECASTING_GRANULARITY_SECS) as usize;
        let mut forecasts = vec![100.0; num_points];
        // For the 2 minutes after, forecast 1 invocations per second.
        let num_points = (2.0 * 60.0 / FORECASTING_GRANULARITY_SECS) as usize;
        forecasts.extend(vec![1.0; num_points]);
        let forecast = (now, forecasts);
        let forecast = Some(forecast);
        rescale_with_metrics_and_forecast(
            &fn_rescaler,
            &mut handler_state,
            &mut subsys_state,
            init_lambda_metrics,
            forecast,
        )
        .await;
        // The invocation rate should be 100.
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.invocation_rate > 99.0 && scaling_info.invocation_rate < 101.0);
        // Even if I pass specific metrics, the invocation rate should still be 100.0;
        for _ in 0..100 {
            let metrics = test_metrics(10, min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.invocation_rate > 99.0 && scaling_info.invocation_rate < 101.0);
        // Wait for next phase to start.
        println!("Waiting for next phase to start (2 minutes)...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2 * 60)).await;
        // At forecast boundary, should avoid decreasing. So still 100.0.
        rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, vec![]).await;
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        println!("Scaling info: {scaling_info:?}");
        assert!(scaling_info.invocation_rate > 99.0 && scaling_info.invocation_rate < 101.0);
        // Cross boundary.
        println!("Crossing boundary");
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(
            2.0 * FORECASTING_GRANULARITY_SECS,
        ))
        .await;
        // The invocation rate should be 1.0, regardless of the metrics passed in.
        rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, vec![]).await;
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        println!("Scaling info: {scaling_info:?}");
        assert!(scaling_info.invocation_rate > 0.9 && scaling_info.invocation_rate < 1.1);
        // Wait for forecast to end.
        println!("Waiting for forecast to end (3 minutes)...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3 * 60)).await;
        // Now the metrics should be used.
        for _ in 0..100 {
            let metrics = test_metrics(2 * 10, min_mem_mb, false);
            rescale_with_metrics(&fn_rescaler, &mut handler_state, &mut subsys_state, metrics)
                .await;
        }
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.invocation_rate > 1.9 && scaling_info.invocation_rate < 2.1);
    }
}
