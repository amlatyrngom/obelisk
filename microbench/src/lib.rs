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
    // use messaging::MessagingClient;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    const USER_MEM: i32 = 512;

    struct RequestSender {
        is_fn: bool,
        is_sim: bool,
        curr_avg_latency: f64,
        desired_requests_per_second: f64,
        fc: Arc<FunctionalClient>,
    }

    impl RequestSender {
        // Next 5 seconds of requests
        async fn send_request_window(&mut self) -> Vec<(Duration, Vec<u8>)> {
            // Window duration.
            let window_duration = 5.0;
            let num_needed_threads =
                (self.desired_requests_per_second * self.curr_avg_latency).ceil();
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
            self.curr_avg_latency = 0.9 * self.curr_avg_latency + 0.1 * avg_duration;
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
            curr_avg_latency: 0.025,
            desired_requests_per_second: 0.0,
            fc: fc.clone(),
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
}
