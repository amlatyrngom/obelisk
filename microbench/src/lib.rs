pub mod micro_actor;
pub use micro_actor::MicroActor;
pub mod micro_function;
pub use micro_function::MicroFunction;
pub mod runner;
pub use runner::BenchRunner;

pub async fn prepare_deployment() -> Vec<String> {
    // Return specs.
    let micro = include_str!("micro.toml");
    let runner = include_str!("runner.toml");
    vec![micro.into(), runner.into()]
}

#[cfg(test)]
mod tests {
    use crate::micro_actor::MicroActorReq;
    use functional::FunctionalClient;
    use messaging::MessagingClient;
    use std::sync::Arc;
    use std::time::Duration;

    /// Write bench output.
    async fn write_bench_output(points: Vec<(u64, f64, String)>, expt_name: &str) {
        let expt_dir = "results/microactor_bench";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, duration, operation) in points {
            writer
                .write_record(&[since.to_string(), duration.to_string(), operation])
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

    async fn run_bench(fc: Arc<FunctionalClient>, rate: RequestRate, prefix: &str) {
        let (activity, num_workers) = match rate {
            RequestRate::Low => (0.1, 1),
            RequestRate::Medium => (1.0, 1),
            RequestRate::High(num_workers) => (1.0, num_workers),
        };
        let test_duration = std::time::Duration::from_secs(60 * 15); // 15 minutes.
        let mut workers = Vec::new();
        for n in 0..num_workers {
            let fc = fc.clone();
            let test_duration = test_duration.clone();
            workers.push(tokio::spawn(async move {
                let mut results: Vec<(u64, f64, String)> = Vec::new();
                let start_time = std::time::Instant::now();
                let req: String = "message".into();
                let req = serde_json::to_vec(&req).unwrap();
                loop {
                    let curr_time = std::time::Instant::now();
                    let since = curr_time.duration_since(start_time);
                    if since > test_duration {
                        break;
                    }
                    let since = since.as_millis() as u64;
                    let resp = fc.invoke(&req).await;
                    if resp.is_err() {
                        println!("Err: {resp:?}");
                        continue;
                    }
                    let resp = resp.unwrap();
                    let resp: (Vec<Duration>, Vec<Duration>, Vec<i64>) =
                        serde_json::from_value(resp).unwrap();
                    let mut active_time_ms: f64 = 0.0; // Used to determine sleep time.

                    if n < 2 {
                        // Avoid too many prints.
                        println!("Worker {n} Resp: {resp:?}");
                        println!("Worker {n} Since: {since:?}");
                    }
                    let (retrieve_times, increment_times, _vals) = resp;
                    for duration in retrieve_times {
                        active_time_ms += 10.0; // Unconditionally set 10.0 to simulate Lambda.
                        results.push((since, duration.as_secs_f64(), "Retrieve".into()));
                    }
                    for duration in increment_times {
                        active_time_ms += 10.0 + 5.0; // Set to 5.0 to simulate Lambda.
                        results.push((since, duration.as_secs_f64(), "Increment".into()));
                    }
                    active_time_ms += 40.0; // NOTE: Remove when running from within AWS.
                    let mut wait_time_ms = active_time_ms / activity - active_time_ms;
                    if wait_time_ms > 10.0 * 1000.0 {
                        wait_time_ms = 10.0 * 1000.0; // Prevent excessive waiting.
                    }
                    if wait_time_ms > 1.0 {
                        let wait_time =
                            std::time::Duration::from_millis(wait_time_ms.ceil() as u64);
                        tokio::time::sleep(wait_time).await;
                    }
                }
                results
            }));
        }
        let mut results = Vec::new();
        for w in workers {
            let mut r = w.await.unwrap();
            results.append(&mut r);
        }
        write_bench_output(results, &format!("{prefix}_{rate:?}")).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_test_cloud() {
        let mc = Arc::new(MessagingClient::new("microbench", "main").await);
        // Retrieve.
        let req = MicroActorReq::Retrieve;
        let req = serde_json::to_string(&req).unwrap();
        let start_time = std::time::Instant::now();
        let resp = mc.send_message(&req, &[]).await;
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Retrieve Resp: {resp:?}");
        println!("Retrieve Duration: {duration:?}");
        // Increment
        let req = MicroActorReq::Increment(10);
        let req = serde_json::to_string(&req).unwrap();
        let start_time = std::time::Instant::now();
        let resp = mc.send_message(&req, &[]).await;
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Increment Resp: {resp:?}");
        println!("Increment Duration: {duration:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_cloud() {
        let fc = Arc::new(FunctionalClient::new("microrunner").await);
        let req: String = "message".into();
        let req = serde_json::to_vec(&req).unwrap();
        let resp = fc.invoke(&req).await.unwrap();
        let resp: (Vec<Duration>, Vec<Duration>, Vec<i64>) = serde_json::from_value(resp).unwrap();
        println!("Response: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_bench_cloud() {
        let fc = Arc::new(FunctionalClient::new("microrunner").await);
        run_bench(fc.clone(), RequestRate::Low, "pre").await;
        run_bench(fc.clone(), RequestRate::Medium, "pre").await;
        run_bench(fc.clone(), RequestRate::High(10), "pre").await;
        run_bench(fc.clone(), RequestRate::Low, "post").await;
    }
}
