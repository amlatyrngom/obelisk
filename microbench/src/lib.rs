pub mod micro_actor;
pub use micro_actor::MicroActor;
pub mod micro_function;
pub use micro_function::MicroFunction;
pub mod runner;
pub use runner::BenchRunner;

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

    async fn run_bench(
        fc: Arc<FunctionalClient>,
        rate: RequestRate,
        prefix: &str,
        test_duration: Duration,
    ) {
        let (activity, num_workers, per_request_count) = match rate {
            RequestRate::Low => (0.1, 1, 5),
            RequestRate::Medium => (1.0, 1, 10),
            RequestRate::High(num_workers) => (1.0, num_workers, 10),
        };
        let mut workers = Vec::new();
        for n in 0..num_workers {
            let fc = fc.clone();
            let test_duration = test_duration.clone();
            workers.push(tokio::spawn(async move {
                let mut results: Vec<(u64, f64, String)> = Vec::new();
                let start_time = std::time::Instant::now();
                let req = RunnerReq::Actor(per_request_count);
                let meta = serde_json::to_string(&req).unwrap();
                loop {
                    let curr_time = std::time::Instant::now();
                    let since = curr_time.duration_since(start_time);
                    if since > test_duration {
                        break;
                    }
                    let since = since.as_millis() as u64;
                    let start_time = std::time::Instant::now();
                    let resp = fc.invoke(&meta, &[]).await;
                    let end_time = std::time::Instant::now();
                    let total_rtt_secs = end_time.duration_since(start_time).as_secs_f64();
                    let (resp_meta, _) = resp.unwrap();
                    let resp: (Vec<Duration>, Vec<Duration>, Vec<i64>) =
                        serde_json::from_str(&resp_meta).unwrap();
                    if n < 2 {
                        // Avoid too many prints.
                        println!("Worker {n} Resp: {resp:?}");
                        println!("Worker {n} Since: {since:?}");
                    }
                    let (retrieve_times, increment_times, _vals) = resp;
                    let retrieve_duration = retrieve_times
                        .iter()
                        .fold(Duration::from_secs(1), |acc, d| acc + *d);
                    let retrieve_duration = retrieve_duration.div_f64(retrieve_times.len() as f64);
                    let increment_duration = increment_times
                        .iter()
                        .fold(Duration::from_secs(1), |acc, d| acc + *d);
                    let increment_duration =
                        increment_duration.div_f64(increment_times.len() as f64);
                    results.push((since, retrieve_duration.as_secs_f64(), "Retrieve".into()));
                    results.push((since, increment_duration.as_secs_f64(), "Increment".into()));
                    // Compute wait time to get desired activity.
                    let mut wait_time_secs = total_rtt_secs / activity - total_rtt_secs;
                    if wait_time_secs > 20.0 {
                        wait_time_secs = 20.0;
                    }
                    if wait_time_secs > 1e-4 {
                        let wait_time = std::time::Duration::from_secs_f64(wait_time_secs);
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_cloud() {
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        let req = RunnerReq::Actor(5);
        let meta = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&meta, &[]).await.unwrap();
        let resp: (Vec<Duration>, Vec<Duration>, Vec<i64>) = serde_json::from_str(&resp).unwrap();
        println!("Response: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_bench_cloud() {
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        run_bench(
            fc.clone(),
            RequestRate::Low,
            "pre",
            Duration::from_secs_f64(100.0),
        )
        .await;
        run_bench(
            fc.clone(),
            RequestRate::Medium,
            "pre",
            Duration::from_secs_f64(100.0),
        )
        .await;
        run_bench(
            fc.clone(),
            RequestRate::High(4),
            "pre",
            Duration::from_secs_f64(200.0),
        )
        .await;
        // run_bench(fc.clone(), RequestRate::Medium, "pre").await;
        // run_bench(fc.clone(), RequestRate::High(10), "pre").await;
        // run_bench(fc.clone(), RequestRate::Low, "post").await;
    }
}
