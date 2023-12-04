use std::collections::{BTreeMap, HashMap};

use common::{HandlerScalingState, Rescaler, RescalingResult, SubsystemScalingState};
use serde::{Deserialize, Serialize};

/// Factor for moving average.
const MOVING_FACTOR: f64 = 0.25;
/// Overhead of lambda.
const AVG_LAMBDA_OVERHEAD_SECS: f64 = 0.010;
/// Utilization upper bound. Scale up when either cpu or mem above this.
const UTILIZATION_UPPER_BOUND: f64 = 0.85;
/// Utilization lower bound. Scale down when both cpu and mem below this.
const UTILIZATION_LOWER_BOUND: f64 = 0.25;
/// Number of rescaling rounds to observe before considering utilization stable.
/// A rescaling round occurs once every ~10 seconds (see constants).
const UTILIZATION_NUM_ROUNDS: f64 = 10.0;
/// Upscaling for safety.
const UPSCALE_FACTOR: f64 = 0.0;

/// Exploration stats.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ExplorationStats {
    num_points: f64,
    avg_latency: f64,
}

/// Info to maintain for scaling functions.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct FunctionalScalingInfo {
    invocation_rate: f64,
    caller_mem_avg: f64,
    lambda_stats: ExplorationStats,
    exploration_stats: BTreeMap<i32, ExplorationStats>,
    scheduled_scale_downs: BTreeMap<i32, chrono::DateTime<chrono::Utc>>,
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
        // Get handling state.
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
                invocation_rate: 0.0,
                caller_mem_avg: 0.0,
                lambda_stats: ExplorationStats {
                    num_points: 0.0,
                    avg_latency: 0.0,
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
                                avg_latency: 0.0,
                            },
                        )
                    })
                    .collect(),
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
            let (lambda_price, ecs_price, invoker_price, _observed_concurrency) = self
                .summarize_stats(
                    &current_stats,
                    &subsystem_scaling_state,
                    &handler_scaling_state,
                    ideal_mem,
                );
            // If price is ok, break.
            // TODO: Exclude invoker in unique calculation.
            if ecs_price + invoker_price <= lambda_price {
                break;
            }
            // If price is too high, try next mem.
            ideal_mem /= 2;
        }
        // Resummarize using new ideal mem in case it changed.
        let (lambda_price, ecs_price, invoker_price, observed_concurrency) = self.summarize_stats(
            &current_stats,
            &subsystem_scaling_state,
            &handler_scaling_state,
            ideal_mem,
        );
        // If base price still too high shutdown everything.
        if ecs_price + invoker_price > lambda_price {
            self.schedule_shutdowns(
                &mut current_stats,
                &handler_scaling_state,
                &mut rescaling_result,
                None,
            );
        } else {
            // If base price enough, shutdown everything except ideal instance type.
            self.schedule_shutdowns(
                &mut current_stats,
                &handler_scaling_state,
                &mut rescaling_result,
                Some(ideal_mem),
            );
        }
        // Determines the number of base containers we can deploy in a cost effective manner and still be able to handle the load.
        let cost_ratio = (lambda_price - invoker_price) / ecs_price;
        let target_scale =
            observed_concurrency / handler_scaling_state.handler_spec.concurrency as f64;
        let target_scale = target_scale * (UPSCALE_FACTOR + 1.0);
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
        println!("Summary: LP={lambda_price}, EP={ecs_price}, IP={invoker_price}, OC={observed_concurrency}, CR={cost_ratio}, TS={target_scale}, TD={to_deploy}, IM={ideal_mem}");
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
        handler_scaling_state: &HandlerScalingState,
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
        for metric in &metrics {
            let metric: FunctionalMetric = bincode::deserialize(metric).unwrap();
            // Update stats.
            num_invocations += 1.0;
            caller_mem += metric.caller_mem as f64;
            // Update exploration stats.
            let duration_secs = metric.duration.as_secs_f64();
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
        }
        num_invocations /= since;
        println!("Invocations/second: {num_invocations:?}");
        // Update global stats.
        current_stats.invocation_rate =
            (1.0 - MOVING_FACTOR) * current_stats.invocation_rate + MOVING_FACTOR * num_invocations;
        current_stats.caller_mem_avg =
            (1.0 - MOVING_FACTOR) * current_stats.caller_mem_avg + MOVING_FACTOR * caller_mem;
        // Update lambda stats.
        lambda_exploration.avg_latency /= lambda_exploration.num_points;
        current_stats.lambda_stats.avg_latency =
            (1.0 - MOVING_FACTOR) * current_stats.lambda_stats.avg_latency
                + MOVING_FACTOR * lambda_exploration.avg_latency;
        current_stats.lambda_stats.num_points += lambda_exploration.num_points;
        // Update ecs stats.
        for (mem, exploration) in &mut current_stats.exploration_stats {
            if let Some(new_exploration) = ecs_explorations.get(&mem) {
                new_exploration.avg_latency /= new_exploration.num_points;
                exploration.avg_latency = (1.0 - MOVING_FACTOR) * exploration.avg_latency
                    + MOVING_FACTOR * new_exploration.avg_latency;
                exploration.num_points += new_exploration.num_points;
            }
        }
    }

    fn get_mems_to_explore(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
    ) -> Vec<i32> {
        let mut res = Vec::new();
        let mut min_observed_latency = current_stats.avg_call_latency;
        let avg_user_mem = current_stats.user_activity_gbsec / AVG_LAMBDA_OVERHEAD_SECS;
        let mut min_instance_cost: Option<f64> = None;
        for (mem, exploration) in &current_stats.exploration_stats {
            // Assume larger instance sizes with missing info will have latency similar to fastest one seen so far.
            // This assumes iteration is in key order.
            let observed_latency = if exploration.num_points >= UTILIZATION_NUM_ROUNDS {
                exploration.avg_call_latency
            } else {
                min_observed_latency
            };
            if min_observed_latency > observed_latency {
                min_observed_latency = observed_latency;
            }
            // Compute user cost under the observed latency.
            let user_activity_gbsec = current_stats.avg_call_rate * observed_latency * avg_user_mem;
            let user_cost = user_activity_gbsec * 0.0000166667 * 3600.0;
            // Compute cost.
            let (ecs_price, invoker_price) =
                self.get_instance_cost(&current_stats, subsys_state, handler_scaling_state, *mem);
            // Total cost
            let total_cost = user_cost + ecs_price + invoker_price;
            if min_instance_cost.is_none() {
                min_instance_cost = Some(total_cost);
            }
            let max_allowable_cost =
                (1.0 + handler_scaling_state.handler_spec.scaleup) * min_instance_cost.unwrap();
            if total_cost <= max_allowable_cost {
                res.push(*mem);
            }
        }
        res
    }

    /// Find first instance size that is not over utilized.
    fn find_ideal_memory(
        &self,
        _current_stats: &mut FunctionalScalingInfo,
        _handler_scaling_state: &HandlerScalingState,
        to_explore: &[i32],
    ) -> i32 {
        // // For testing: Just return the maximum allowable memory.
        return *to_explore.last().unwrap();
        // let mut previous_mem = None;
        // let min_mem = *to_explore.first().unwrap();
        // let max_mem = *to_explore.last().unwrap();
        // let mut cpu_bound = false;
        // for mem in to_explore {
        //     let mem = *mem;
        //     let exploration = current_stats.exploration_stats.get_mut(&mem).unwrap();
        //     // Special case: Skip first scale up container if cpu bound.
        //     if cpu_bound && mem < max_mem && mem == 2 * min_mem {
        //         continue;
        //     }
        //     // Check if enough points have been collected.
        //     if exploration.num_points < UTILIZATION_NUM_ROUNDS {
        //         // Use this until more points are collected.
        //         return mem;
        //     }

        //     // Check overutilization unless already at max size.
        //     if mem < max_mem
        //         && (exploration.avg_cpu_util > UTILIZATION_UPPER_BOUND
        //             || exploration.avg_mem_util > UTILIZATION_UPPER_BOUND)
        //     {
        //         previous_mem = Some(mem);
        //         cpu_bound = exploration.avg_cpu_util > UTILIZATION_UPPER_BOUND;
        //         continue;
        //     }
        //     // Check underutilization.
        //     // If underutilized, use previous instance size.
        //     if exploration.avg_cpu_util < UTILIZATION_LOWER_BOUND
        //         && exploration.avg_mem_util < UTILIZATION_LOWER_BOUND
        //     {
        //         if previous_mem.is_none() {
        //             // If no smaller size, return.
        //             return mem;
        //         }
        //         // Reset exploration.
        //         exploration.num_points = 0.0;
        //         exploration.avg_cpu_util = 0.0;
        //         exploration.avg_mem_util = 0.0;
        //         break;
        //     }
        //     // Well utilized.
        //     return mem;
        // }
        // if let Some(previous_mem) = previous_mem {
        //     // Reset exploration.
        //     let exploration = current_stats
        //         .exploration_stats
        //         .get_mut(&previous_mem)
        //         .unwrap();
        //     exploration.num_points = 0.0;
        //     exploration.avg_cpu_util = 0.0;
        //     exploration.avg_mem_util = 0.0;
        //     previous_mem
        // } else {
        //     min_mem
        // }
    }

    /// Schedule the right shutdowns.
    fn schedule_shutdowns(
        &self,
        current_stats: &mut FunctionalScalingInfo,
        handler_scaling_state: &HandlerScalingState,
        rescaling_result: &mut RescalingResult,
        ideal_mem: Option<i32>,
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
                    .insert(*mem, now + chrono::Duration::minutes(1));
                continue;
            }
            // If already scheduled, check if should shutdown.
            let scale_down_time = current_stats.scheduled_scale_downs.get(mem).unwrap();
            if *scale_down_time < now {
                println!("Actually shutting down: {mem}");
                handler_scales.insert(*mem, 0);
            }
        }
    }

    fn get_instance_cost(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
        mem: i32,
    ) -> (f64, f64) {
        // Compute ecs cost.
        let min_mem = *current_stats.exploration_stats.keys().min().unwrap();
        let cpus = if mem == min_mem { mem / 2 } else { mem / 4 };
        let ecs_mem_gb = mem as f64 / 1024.0;
        let ecs_cpus = cpus as f64 / 1024.0;
        let ecs_price = (0.01234398 * ecs_cpus) + (0.00135546 * ecs_mem_gb);
        // Invoker is only needed for non-unique functions (non-actors).
        let invoker_price = if handler_scaling_state.handler_spec.unique {
            0.0
        } else {
            let invoker_specs = subsys_state.service_specs.get("invoker").unwrap();
            let invoker_mem = invoker_specs.mem as f64 / 1024.0;
            let invoker_cpus = invoker_specs.cpus as f64 / 1024.0;
            (0.01234398 * invoker_cpus) + (0.00135546 * invoker_mem)
        };
        return (ecs_price, invoker_price);
    }

    /// Summarize current statistics.
    fn summarize_stats(
        &self,
        current_stats: &FunctionalScalingInfo,
        subsys_state: &SubsystemScalingState,
        handler_scaling_state: &HandlerScalingState,
        ideal_mem: i32,
    ) -> (f64, f64, f64, f64) {
        // Compute lambda cost.
        let default_mem_gb = handler_scaling_state.handler_spec.default_mem as f64 / 1024.0;
        let caller_mem_gb = current_stats.caller_mem_avg / 1024.0;
        let fn_call_cost = current_stats.invocation_rate * 0.2 * 3600.0 / 1e6;
        let fn_activity = current_stats.invocation_rate * current_stats.lambda_stats.avg_latency;
        let fn_activity_gbsec = fn_activity * default_mem_gb;
        let fn_compute_cost = fn_activity_gbsec * 0.0000166667 * 3600.0;
        let fn_user_activity = current_stats.invocation_rate * (current_stats.lambda_stats.avg_latency + AVG_LAMBDA_OVERHEAD_SECS);
        let fn_user_activity_gbsec = fn_user_activity * caller_mem_gb;
        let fn_user_cost = fn_user_activity_gbsec * 0.0000166667 * 3600.0;
        let lambda_price = fn_user_cost + fn_compute_cost + fn_call_cost;
        let observed_concurrency: f64 = fn_activity;
        // Compute ecs cost.
        let (ecs_price, invoker_price) = self.get_instance_cost(
            current_stats,
            subsys_state,
            handler_scaling_state,
            ideal_mem,
        );
        return (lambda_price, ecs_price, invoker_price, observed_concurrency);
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
        // Special case for actors.
        if handler_scaling_state.handler_spec.unique {
            rescaling_result.services_scales.insert("invoker".into(), 0);
            to_deploy = 1;
        } else {
            println!("Setting service scales");
            rescaling_result.services_scales.insert("invoker".into(), 1);
        };
        // Update scales.
        let handler_scales = rescaling_result.handler_scales.as_mut().unwrap();
        handler_scales.insert(ideal_mem, to_deploy as u64);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common::deployment::{container, HandlerSpec, ServiceSpec};
    use common::Rescaler;

    use crate::rescaler::FunctionalScalingInfo;

    use super::{FunctionalMetric, FunctionalRescaler, UTILIZATION_LOWER_BOUND};
    use super::{HandlerScalingState, SubsystemScalingState};

    fn test_handler_state(scaleup: f64) -> HandlerScalingState {
        let mems = container::ContainerDeployment::all_avail_mems(512, scaleup);
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
                default_mem: 512,
                concurrency: 10,
                ephemeral: 512,
                persistent: false,
                unique: false,
                scaleup,
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

    fn test_metrics(num_metrics: usize, mem_size_mb: i32, util: f64) -> Vec<Vec<u8>> {
        let mut metrics = Vec::new();
        // Prevent cpu overutilization in test, but allow underutilization.
        // This is because cpu overutilization has more complex scale up/down.
        let cpu_usage = if util < UTILIZATION_LOWER_BOUND {
            util
        } else {
            0.5
        };
        for _ in 0..num_metrics {
            let metric = FunctionalMetric {
                duration: std::time::Duration::from_secs(1),
                mem_size_mb,
                mem_usage: Some(util),
                cpu_usage: Some(cpu_usage),
                caller_mem: 512,
            };
            let metric = bincode::serialize(&metric).unwrap();
            metrics.push(metric);
        }
        metrics
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
        let since_last_rescaling = std::time::Duration::from_secs(10);
        let min_mem_mb = 1024;
        for _ in 0..100 {
            let metrics = test_metrics(10, min_mem_mb, 0.5);
            // println!("Before Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
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
            // println!("After Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.avg_call_rate > 0.9 && scaling_info.avg_call_rate < 1.1);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        let handler_scales = compute_actual_scales(&handler_state);
        // assert!(handler_scales.get(&512).unwrap() == &1);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &1); // 1024 is minimum size.
        for _ in 0..100 {
            let metrics = vec![];
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
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After PostHandler:", &handler_state);
        let scaling_info = handler_state.scaling_info.as_ref().unwrap();
        let scaling_info: FunctionalScalingInfo = serde_json::from_str(scaling_info).unwrap();
        assert!(scaling_info.avg_call_rate < 0.01);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &0);
        let handler_scales = compute_actual_scales(&handler_state);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &0);
        for _ in 0..100 {
            // Since batching is set to 10, need >100 calls in 10seconds to set target scale to 2.0;
            let metrics = test_metrics(10 * 15, 512, 0.5);
            // println!("Before Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
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
            // println!("After Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        let handler_scales = compute_actual_scales(&handler_state);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        assert!(handler_scales.get(&min_mem_mb).unwrap() == &2);
    }

    /// Increase and decrease utilization to see if scaling happens correctly.
    #[tokio::test]
    async fn basic_usage_test() {
        run_basic_usage_test().await;
    }

    async fn run_basic_usage_test() {
        let fn_rescaler = FunctionalRescaler::new().await;
        let mut subsys_state = test_subsys_state();
        let mut handler_state = test_handler_state(1.0);
        let since_last_rescaling = std::time::Duration::from_secs(10);
        let min_mem_mb = 1024;
        let scaled_mem_mb = 2 * min_mem_mb;
        for _ in 0..100 {
            // Make overutilized.
            let metrics = test_metrics(10, min_mem_mb, 1.0);
            // println!("Before Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
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
            // println!("After Subsys: {subsys_state:?}");
            // show_handler_state("Before Handler:", &handler_state);
        }
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        // Before scheduled time, both instances should be active. After, only the larger one.
        let pre_handler_scales = handler_state.handler_scales.clone();
        let post_handler_scales = compute_actual_scales(&handler_state);
        assert!(pre_handler_scales.get(&min_mem_mb).unwrap() == &1);
        // assert!(pre_handler_scales.get(&scaled_mem_mb).unwrap() == &1);
        assert!(post_handler_scales.get(&min_mem_mb).unwrap() == &0);
        // assert!(post_handler_scales.get(&scaled_mem_mb).unwrap() == &1);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        for _ in 0..100 {
            // Now make 1024 underutilize. Show scale down to 512MB.
            // Also set high oberseved concurrency to see if both change.
            let metrics = test_metrics(1, 2 * min_mem_mb, 0.1);
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
        println!("After Subsys: {subsys_state:?}");
        show_handler_state("After Handler:", &handler_state);
        // Before scheduled time, both instances should be active. After, only the larger one.
        let pre_handler_scales = handler_state.handler_scales.clone();
        let post_handler_scales = compute_actual_scales(&handler_state);
        assert!(pre_handler_scales.get(&min_mem_mb).unwrap() == &2);
        assert!(pre_handler_scales.get(&scaled_mem_mb).unwrap() > &0);
        assert!(post_handler_scales.get(&min_mem_mb).unwrap() == &2);
        assert!(post_handler_scales.get(&scaled_mem_mb).unwrap() == &0);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
        // Now wait for 1 minute. Should shutdown the larger instance.
        println!("Waiting for scheduled scale downs.");
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        {
            let metrics = test_metrics(10 * 15, 2 * min_mem_mb, 0.1);
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
        let pre_handler_scales = handler_state.handler_scales.clone();
        assert!(pre_handler_scales.get(&min_mem_mb).unwrap() == &2);
        assert!(pre_handler_scales.get(&scaled_mem_mb).unwrap() == &0);
        assert!(subsys_state.service_scales.get("invoker").unwrap() == &1);
    }
}
