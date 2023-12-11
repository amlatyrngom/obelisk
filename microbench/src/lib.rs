pub mod micro_actor;
pub use micro_actor::MicroActor;
pub mod micro_function;
pub use micro_function::EchoFn;
pub use micro_function::MicroFunction;
pub mod runner;
pub use runner::BenchRunner;
pub mod sim_actor;
pub use sim_actor::SimActor;

pub async fn prepare_deployment() -> Vec<String> {
    // Return specs.
    let deployment = include_str!("deployment.toml");
    vec![deployment.into()]
}

#[cfg(test)]
mod tests {
    use crate::micro_actor::MicroActorReq;
    use crate::runner::RunnerReq;
    use functional::FunctionalClient;
    use std::os::unix::thread;
    // use messaging::MessagingClient;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    const USER_MEM: i32 = 512;
    use common::metrics::MetricsManager;
    use functional::rescaler::FORECASTING_GRANULARITY_SECS;

    struct RequestSender {
        is_fn: bool,
        is_sim: bool,
        curr_avg_latency: f64,
        desired_requests_per_second: f64,
        thread_cap: f64,
        fc: Arc<FunctionalClient>,
    }

    impl RequestSender {
        // Next 5 seconds of requests
        async fn send_request_window(&mut self) -> Vec<(Duration, Vec<u8>)> {
            // Window duration.
            let window_duration = 5.0;
            let mut num_needed_threads =
                (self.desired_requests_per_second * self.curr_avg_latency).ceil();
            if num_needed_threads > self.thread_cap {
                // Use to manually prevent wrong latency estimate from ruining the benchmark.
                num_needed_threads = self.thread_cap;
            }
            let total_num_requests = window_duration * self.desired_requests_per_second;
            let requests_per_thread = (total_num_requests / num_needed_threads).ceil();
            let actual_total_num_requests = requests_per_thread * num_needed_threads;
            let num_needed_threads = num_needed_threads as u64;
            println!("NT={num_needed_threads}; RPT={requests_per_thread};");
            let mut ts = Vec::new();
            let overall_start_time = Instant::now();
            for _ in 0..num_needed_threads {
                let requests_per_thread = requests_per_thread as u64;
                let fc = self.fc.clone();
                let is_fn = self.is_fn;
                let is_sim: bool = self.is_sim;
                let t = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let mut responses = Vec::new();
                    let mut curr_idx = 0;
                    while curr_idx < requests_per_thread {
                        // Find number of calls to make and update curr idx.
                        let batch_size = 10;
                        let call_count = if requests_per_thread - curr_idx < batch_size {
                            requests_per_thread - curr_idx
                        } else {
                            batch_size
                        };
                        curr_idx += call_count;
                        // Now send requests.
                        let req = if is_fn {
                            RunnerReq::Function(call_count)
                        } else if is_sim {
                            RunnerReq::Sim(call_count)
                        } else {
                            RunnerReq::Actor(call_count)
                        };
                        let meta = serde_json::to_string(&req).unwrap();
                        let resp = fc.invoke(&meta, &[]).await;
                        if resp.is_err() {
                            println!("Err: {resp:?}");
                            continue;
                        }
                        let (resp, _) = resp.unwrap();
                        let mut resp: Vec<(Duration, Vec<u8>)> =
                            serde_json::from_str(&resp).unwrap();
                        responses.append(&mut resp);
                    }
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);

                    (duration, responses)
                });
                ts.push(t);
            }
            let mut sum_duration = Duration::from_millis(0);
            let mut all_responses = Vec::new();
            for t in ts {
                let (duration, mut responses) = t.await.unwrap();
                sum_duration = sum_duration.checked_add(duration).unwrap();
                all_responses.append(&mut responses);
            }
            let avg_duration = sum_duration.as_secs_f64() / (actual_total_num_requests);
            let factor = 0.75;
            self.curr_avg_latency = factor * self.curr_avg_latency + (1.0 - factor) * avg_duration;
            let mut avg_req_latency =
                all_responses.iter().fold(Duration::from_secs(0), |acc, x| {
                    acc.checked_add(x.0.clone()).unwrap()
                });
            if all_responses.len() > 0 {
                avg_req_latency = avg_req_latency.div_f64(all_responses.len() as f64);
            }
            println!(
                "AVG_LATENCY={avg_duration}; CURR_AVG_LATENCY={}; REQ_LATENCY={avg_req_latency:?}",
                self.curr_avg_latency
            );
            let overall_end_time = Instant::now();
            let overall_duration = overall_end_time.duration_since(overall_start_time);
            if overall_duration.as_secs_f64() < window_duration {
                let sleep_duration =
                    Duration::from_secs_f64(window_duration - overall_duration.as_secs_f64());
                println!("Window sleeping for: {:?}.", sleep_duration);
                tokio::time::sleep(sleep_duration).await;
            }
            all_responses
        }
    }

    /// Write bench output.
    async fn write_bench_output(points: Vec<(u64, f64, Vec<u8>)>, expt_name: &str, _is_fn: bool) {
        let expt_dir = "results/microbench";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, duration, metadata) in points {
            let (mem, is_lambda): (i32, bool) = serde_json::from_slice(&metadata).unwrap();
            let mode = if is_lambda { "Lambda" } else { "ECS" };
            writer
                .write_record(&[
                    since.to_string(),
                    duration.to_string(),
                    mem.to_string(),
                    mode.to_string(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    #[derive(Debug)]
    enum RequestRate {
        Low,
        Medium,
        High(usize),
    }

    async fn run_bench(request_sender: &mut RequestSender, name: &str, test_duration: Duration) {
        let mut results: Vec<(u64, f64, Vec<u8>)> = Vec::new();
        let start_time = std::time::Instant::now();
        loop {
            // Pick an image at random.
            // TODO: Find a better way to select images.
            let curr_time = std::time::Instant::now();
            let since = curr_time.duration_since(start_time);
            if since > test_duration {
                break;
            }
            let since = since.as_millis() as u64;
            let resp: Vec<(Duration, Vec<u8>)> = request_sender.send_request_window().await;
            for (duration, metadata) in &resp {
                results.push((since, duration.as_secs_f64(), metadata.clone()));
            }
        }
        write_bench_output(results, &format!("{name}"), request_sender.is_fn).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test_cloud() {
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await);
        // Retrieve.
        let req = MicroActorReq::Retrieve;
        let req = serde_json::to_string(&req).unwrap();
        let start_time = std::time::Instant::now();
        let resp = fc.invoke(&req, &[]).await;
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Retrieve Resp: {resp:?}");
        println!("Retrieve Duration: {duration:?}");
        // Increment
        let req = MicroActorReq::Increment(10);
        let req = serde_json::to_string(&req).unwrap();
        let start_time = std::time::Instant::now();
        let resp = fc.invoke(&req, &[]).await;
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Increment Resp: {resp:?}");
        println!("Increment Duration: {duration:?}");
        // Decrement
        let req = MicroActorReq::Increment(-10);
        let req = serde_json::to_string(&req).unwrap();
        let start_time = std::time::Instant::now();
        let resp = fc.invoke(&req, &[]).await;
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Increment Resp: {resp:?}");
        println!("Increment Duration: {duration:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_fn_cloud() {
        {
            // Make sure stuff is initialized.
            let _fn_client =
                Arc::new(FunctionalClient::new("microbench", "microfn", None, Some(512)).await);
            let _actor_client = Arc::new(
                FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await,
            );
            let _sim_client =
                Arc::new(FunctionalClient::new("microbench", "simactor", Some(0), Some(512)).await);
        }
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        let req = RunnerReq::Function(1);
        let meta = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&meta, &[]).await.unwrap();
        let resp: Vec<(Duration, Vec<u8>)> = serde_json::from_str(&resp).unwrap();
        println!("Response: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_cloud() {
        {
            // Make sure stuff is initialized.
            let _fn_client =
                Arc::new(FunctionalClient::new("microbench", "microfn", None, Some(512)).await);
            let _actor_client = Arc::new(
                FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await,
            );
        }
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        let req = RunnerReq::Actor(1);
        let meta = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&meta, &[]).await.unwrap();
        let resp: Vec<(Duration, Vec<u8>)> = serde_json::from_str(&resp).unwrap();
        println!("Response: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_sim_cloud() {
        {
            // Make sure stuff is initialized.
            let _fn_client =
                Arc::new(FunctionalClient::new("microbench", "microfn", None, Some(512)).await);
            let _actor_client = Arc::new(
                FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await,
            );
            let _sim_client =
                Arc::new(FunctionalClient::new("microbench", "simactor", Some(0), Some(512)).await);
        }
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        let req = RunnerReq::Sim(1);
        let meta = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&meta, &[]).await.unwrap();
        let resp: Vec<(Duration, Vec<u8>)> = serde_json::from_str(&resp).unwrap();
        println!("Response: {resp:?}");
    }

    async fn generate_robustness_workload(
        quiet_intensity: f64,
        quiet_interval: f64,
        burst_intensity: f64,
        shortest_burst_interval: f64,
        num_intervals: usize,
    ) -> (Vec<(f64, f64)>, (chrono::DateTime<chrono::Utc>, Vec<f64>)) {
        let mut workload = Vec::new();
        let mut forecasts = Vec::new();
        let start_time = chrono::Utc::now();
        let mut burst_interval = shortest_burst_interval;
        for _ in 0..num_intervals {
            // Quiet.
            workload.push((quiet_interval, quiet_intensity));
            let num_forecast_points = (quiet_interval / FORECASTING_GRANULARITY_SECS) as usize;
            forecasts.extend(vec![quiet_intensity; num_forecast_points]);
            // Burst.
            workload.push((burst_interval, burst_intensity));
            let num_forecast_points = (burst_interval / FORECASTING_GRANULARITY_SECS) as usize;
            forecasts.extend(vec![burst_intensity; num_forecast_points]);
            // Quiet again.
            workload.push((quiet_interval, quiet_intensity));
            let num_forecast_points = (quiet_interval / FORECASTING_GRANULARITY_SECS) as usize;
            forecasts.extend(vec![quiet_intensity; num_forecast_points]);
            // Double burst interval.
            burst_interval *= 2.0;
        }
        (workload, (start_time, forecasts))
    }

    async fn scaling_state_reader(sm: Arc<common::scaling_state::ScalingStateManager>) {
        loop {
            let scaling_state = sm.retrieve_scaling_state().await.unwrap();
            let scaling_state = scaling_state.handler_state.unwrap();
            if let Some(scaling_info) = scaling_state.scaling_info {
                let scaling_info: functional::rescaler::FunctionalScalingInfo =
                    serde_json::from_str(&scaling_info).unwrap();
                println!("Current scaling info: {scaling_info:?}");
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn test_set_forced_deployment() {
        set_forced_deployment(5.0 * 60.0, 4096, 3, "microbench", "microfn")
            .await
            .unwrap();
    }

    async fn set_forced_deployment(
        duration_secs: f64,
        forced_mem: i32,
        forced_scale: i32,
        namespace: &str,
        name: &str,
    ) -> Result<(), String> {
        let now = chrono::Utc::now();
        let expiry_time = now + chrono::Duration::seconds(duration_secs as i64);
        let forced_deployment = (expiry_time, forced_mem, forced_scale);
        let forced_deployment = bincode::serialize(&forced_deployment).unwrap();
        let mm = MetricsManager::new("functional", namespace, name).await;
        mm.accumulate_metric(forced_deployment).await;
        mm.push_metrics().await.unwrap();
        let sm =
            common::scaling_state::ScalingStateManager::new("functional", namespace, name).await;
        sm.start_rescaling_thread().await;
        for _ in 0..10 {
            match sm.retrieve_scaling_state().await {
                Err(x) => {
                    println!("Error: {x:?}. Retrying...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                Ok(x) => {
                    let scaling_info = x.handler_state.unwrap().scaling_info;
                    if scaling_info.is_none() {
                        println!("Scaling info not set. Retrying...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    let scaling_info = scaling_info.unwrap();
                    let scaling_info: functional::rescaler::FunctionalScalingInfo =
                        serde_json::from_str(&scaling_info).unwrap();
                    let forced_deployment = scaling_info.forced_deployment;
                    if forced_deployment.is_none() {
                        println!("Forced deployment not set. Retrying...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    let forced_deployment = forced_deployment.unwrap();
                    if forced_deployment.0 != expiry_time {
                        println!("Forced deployment corret expiry time not set. Retrying...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    println!("Forced deployment set: {:?}.", forced_deployment);
                    return Ok(());
                }
            }
        }
        Err("Could not set forced deployment.".to_string())
    }

    async fn run_generated_workload(
        workload: Vec<(f64, f64)>,
        forecasts: (chrono::DateTime<chrono::Utc>, Vec<f64>),
        use_forecast: bool,
        thread_cap: f64,
    ) {
        let prefix = if use_forecast {
            // TODO: Push forecast metrics.
            let metrics_manager = MetricsManager::new("functional", "microbench", "microfn").await;
            let forecast = bincode::serialize(&forecasts).unwrap();
            metrics_manager.accumulate_metric(forecast).await;
            let _ = metrics_manager.push_metrics().await;
            "robustness_with_forecast"
        } else {
            "robustness_no_forecast"
        };
        let sm =
            common::scaling_state::ScalingStateManager::new("functional", "microbench", "microfn")
                .await;
        let sm = Arc::new(sm);
        tokio::spawn(scaling_state_reader(sm.clone()));
        let fc = Arc::new(
            FunctionalClient::new("microbench", "microrunner", None, Some(USER_MEM)).await,
        );

        let mut request_sender = RequestSender {
            is_fn: true,
            is_sim: false,
            curr_avg_latency: 0.025,
            desired_requests_per_second: 0.0,
            thread_cap,
            fc: fc.clone(),
        };

        for (i, (time_interval, reqs_per_secs)) in workload.into_iter().enumerate() {
            println!("Running workload: {i}, {time_interval}, {reqs_per_secs}");
            let scaling_state = sm.retrieve_scaling_state().await.unwrap();
            let scaling_state = scaling_state.handler_state.unwrap();
            if let Some(scaling_info) = scaling_state.scaling_info {
                let scaling_info: functional::rescaler::FunctionalScalingInfo =
                    serde_json::from_str(&scaling_info).unwrap();
                println!("Scaling info before: {scaling_info:?}");
            }
            let duration = Duration::from_secs_f64(time_interval);
            let name = format!("{prefix}_{i}");
            request_sender.desired_requests_per_second = reqs_per_secs;
            run_bench(&mut request_sender, &name, duration).await;
            let scaling_state = sm.retrieve_scaling_state().await.unwrap();
            let scaling_state = scaling_state.handler_state.unwrap();
            if let Some(scaling_info) = scaling_state.scaling_info {
                let scaling_info: functional::rescaler::FunctionalScalingInfo =
                    serde_json::from_str(&scaling_info).unwrap();
                println!("Scaling info after: {scaling_info:?}");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn robustness_fn_bench_cloud() {
        let use_forecast = true;
        let quiet_intensity = 4.0; // 4 requests per second.
        let quiet_interval = 60.0 * 5.0;
        let burst_intensity = 400.0; // 400 requests per second.
        let shortest_burst_interval = 60.0; // 60 seconds
        let num_intervals = 3; // 60s, 120s, 240s.
                               // Prevent benchmarker from overadapting. This is manually determined.
        let thread_cap = if use_forecast { 7.0 + 1e-4 } else { 10.0 };
        let (workload, forecasts) = generate_robustness_workload(
            quiet_intensity,
            quiet_interval,
            burst_intensity,
            shortest_burst_interval,
            num_intervals,
        )
        .await;
        run_generated_workload(workload, forecasts, use_forecast, thread_cap).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_fn_bench_cloud() {
        let fc = Arc::new(
            FunctionalClient::new("microbench", "microrunner", None, Some(USER_MEM)).await,
        );
        let duration_mins = 15.0;
        let low_req_per_secs = 1.0;
        let medium_req_per_secs = 40.0;
        let high_req_per_secs = 400.0;
        let mut request_sender = RequestSender {
            is_fn: true,
            is_sim: false,
            curr_avg_latency: 0.025,
            desired_requests_per_second: 0.0,
            fc: fc.clone(),
            thread_cap: 100.0, // Effectively no cap.
        };
        // Low
        request_sender.desired_requests_per_second = low_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_low",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // Medium
        request_sender.desired_requests_per_second = medium_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_medium",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // High
        request_sender.desired_requests_per_second = high_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_high",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // Low again.
        request_sender.desired_requests_per_second = low_req_per_secs;
        run_bench(
            &mut request_sender,
            "post_low",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_actor_bench_cloud() {
        let fc = Arc::new(
            FunctionalClient::new("microbench", "microrunner", None, Some(USER_MEM)).await,
        );
        let duration_mins = 15.0;
        let low_req_per_secs = 1.0;
        let medium_req_per_secs = 40.0;
        let high_req_per_secs = 400.0;
        let mut request_sender = RequestSender {
            is_fn: false,
            is_sim: false,
            curr_avg_latency: 0.020,
            desired_requests_per_second: 0.0,
            fc: fc.clone(),
            thread_cap: 100.0, // Effectively no cap.
        };
        // Low
        request_sender.desired_requests_per_second = low_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_low",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // Medium
        request_sender.desired_requests_per_second = medium_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_medium",
            Duration::from_secs_f64(60.0 * duration_mins / 3.0),
        )
        .await;
        // High
        request_sender.desired_requests_per_second = high_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_high",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // Low again.
        request_sender.desired_requests_per_second = low_req_per_secs;
        run_bench(
            &mut request_sender,
            "post_low",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_sim_bench_cloud() {
        let fc = Arc::new(
            FunctionalClient::new("microbench", "microrunner", None, Some(USER_MEM)).await,
        );
        let duration_mins = 15.0;
        let low_req_per_secs = 1.0;
        let medium_req_per_secs = 20.0;
        let high_req_per_secs = 200.0;
        let mut request_sender = RequestSender {
            is_fn: false,
            is_sim: true,
            curr_avg_latency: 0.025,
            desired_requests_per_second: 0.0,
            fc: fc.clone(),
            thread_cap: 100.0, // Effectively no cap.
        };
        // // Low
        // request_sender.desired_requests_per_second = low_req_per_secs;
        // run_bench(
        //     &mut request_sender,
        //     "pre_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // )
        // .await;
        // Medium
        request_sender.desired_requests_per_second = medium_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_medium",
            Duration::from_secs_f64(60.0 * duration_mins / 3.0),
        )
        .await;
        // High
        request_sender.desired_requests_per_second = high_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_high",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // // Low again.
        // request_sender.desired_requests_per_second = low_req_per_secs;
        // run_bench(
        //     &mut request_sender,
        //     "post_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // )
        // .await;
    }
}
