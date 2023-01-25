use common::{ActorInstance, FunctionInstance};
use functional::FunctionalClient;
use messaging::MessagingClient;
use persistence::PersistentLog;
use serde_json::Value;
use std::sync::Arc;

pub async fn prepare_deployment() -> Vec<String> {
    // Return specs.
    let micro = include_str!("micro.toml");
    let runner = include_str!("runner.toml");
    vec![micro.into(), runner.into()]
}

pub struct BenchRunner {
    msg_mc: Arc<MessagingClient>,
    persist_mc: Arc<MessagingClient>,
    fc: Arc<FunctionalClient>,
}

impl BenchRunner {
    pub async fn new() -> Self {
        let fc = Arc::new(FunctionalClient::new("microbench").await);
        let msg_mc = Arc::new(MessagingClient::new("microbench", "messaging_bench").await);
        let persist_mc = Arc::new(MessagingClient::new("microbench", "persistence_bench").await);
        BenchRunner {
            fc,
            msg_mc,
            persist_mc,
        }
    }
}

#[async_trait::async_trait]
impl FunctionInstance for BenchRunner {
    async fn invoke(&self, arg: Value) -> Value {
        let bench = arg.as_str().unwrap();
        println!("Bench mode: {bench}");
        if bench == "invoke" {
            self.do_invoke_bench().await
        } else if bench == "message" {
            self.do_messaging_bench().await
        } else if bench == "message_payload" {
            self.do_messaging_bench_with_payload().await
        } else if bench == "persist" {
            self.do_persistence_bench().await
        } else {
            let err = format!("Unknown mode, {bench}");
            serde_json::json!({
                "error": err,
            })
        }
    }
}

impl BenchRunner {
    async fn do_invoke_bench(&self) -> Value {
        let arg = serde_json::to_vec("foo").unwrap();
        let start_time = std::time::Instant::now();
        let mut num_direct = 0;
        let mut num_indirect = 0;
        for _ in 0..10 {
            let resp = self.fc.invoke_internal(&arg).await;
            if let Ok((_, is_direct)) = resp {
                if is_direct {
                    num_direct += 1;
                } else {
                    num_indirect += 1;
                }
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        let duration_ms = duration.as_millis() as u64;
        let resp = (duration_ms, num_direct, num_indirect);
        serde_json::to_value(&resp).unwrap()
    }

    async fn do_messaging_bench(&self) -> Value {
        let start_time = std::time::Instant::now();
        let mut num_direct = 0;
        let mut num_indirect = 0;
        for _ in 0..5 {
            let (resp, _is_direct) = self.msg_mc.send_message_internal("foo", &vec![]).await;
            if let Some((execution_mode, _)) = resp {
                if execution_mode == "messaging_ecs" {
                    num_direct += 1;
                } else {
                    num_indirect += 1;
                }
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        let duration_ms = duration.as_millis() as u64;
        let resp = (duration_ms, num_direct, num_indirect);
        serde_json::to_value(&resp).unwrap()
    }

    async fn do_messaging_bench_with_payload(&self) -> Value {
        let start_time = std::time::Instant::now();
        let mut num_direct = 0;
        let mut num_indirect = 0;
        for _ in 0..10 {
            let (resp, is_direct) = self.msg_mc.send_message_internal("foo", &vec![1]).await;
            if let Some(_) = resp {
                if is_direct {
                    num_direct += 1;
                } else {
                    num_indirect += 1;
                }
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        let duration_ms = duration.as_millis() as u64;
        let resp = (duration_ms, num_direct, num_indirect);
        serde_json::to_value(&resp).unwrap()
    }

    async fn do_persistence_bench(&self) -> Value {
        let duration_ms = loop {
            let (resp, _is_direct) = self.persist_mc.send_message_internal("foo", &vec![]).await;
            if let Some((duration_ms, _)) = resp {
                let duration_ms: u64 = duration_ms.parse().unwrap();
                break duration_ms;
            }
        };
        serde_json::to_value(&duration_ms).unwrap()
    }
}

pub struct MicroFunction {}

impl MicroFunction {
    pub async fn new() -> Self {
        MicroFunction {}
    }
}

#[async_trait::async_trait]
impl FunctionInstance for MicroFunction {
    async fn invoke(&self, arg: Value) -> Value {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        arg
    }
}

pub struct MicroActor {
    plog: Arc<PersistentLog>,
    persistence_bench: bool,
}

impl MicroActor {
    /// Create echo actor.
    pub async fn new(name: &str, plog: Arc<PersistentLog>) -> Self {
        println!("Making micro bench actor {name}!");
        let persistence_bench = name == "persistence_bench";
        MicroActor {
            plog,
            persistence_bench,
        }
    }
}

#[async_trait::async_trait]
impl ActorInstance for MicroActor {
    async fn message(&self, _msg: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !self.persistence_bench {
            let execution_mode = std::env::var("EXECUTION_MODE").unwrap();
            return (execution_mode, payload);
        }
        // Make 1 byte flushes and time them.
        let flush_lsn = self.plog.get_flush_lsn().await;
        self.plog.truncate(flush_lsn).await;
        let start_time = std::time::Instant::now();
        for _ in 0..10 {
            self.plog.enqueue(vec![1]).await;
            self.plog.flush(None).await;
            println!("Done Flushing");
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Flushes took: {duration:?}");
        let duration_ms = duration.as_millis();
        (duration_ms.to_string(), vec![])
    }

    async fn checkpoint(&self, terminating: bool) {
        println!("Checkpoint: terminating=({terminating})");
        if terminating {
            let flush_lsn = self.plog.get_flush_lsn().await;
            self.plog.truncate(flush_lsn).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::BenchRunner;
    use functional::FunctionalClient;
    const TEST_MEM: f64 = 2.0; // 2GB.

    async fn write_output(points: Vec<(u64, f64, f64)>, expt_name: &str) {
        let expt_dir = "results/microbench";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, duration_ms, total_cost) in points {
            writer
                .write_record(&[
                    since.to_string(),
                    duration_ms.to_string(),
                    total_cost.to_string(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    async fn do_invoke_round(
        fc: &FunctionalClient,
        start_time: std::time::Instant,
        wait_time: u64,
    ) -> (u64, f64, f64) {
        let mode_name = "invoke";
        let mode = serde_json::to_vec(mode_name).unwrap();
        let timestamp = std::time::Instant::now();
        let since = timestamp.duration_since(start_time).as_secs();
        let resp = fc.invoke_internal(&mode).await;
        println!("Invoke resp: {resp:?}");
        match resp {
            Ok((timing, _is_direct)) => {
                let (total_duration_ms, num_direct, num_indirect): (f64, f64, f64) =
                    serde_json::from_value(timing).unwrap();
                let duration_ms = total_duration_ms / (num_direct + num_indirect);
                // Compute total cost per active ms.
                // For simplification, assume all ecs if any ecs.
                let is_lambda = num_direct < 0.1;
                // Cost of every ms for 2 gb lambda (2vcpu+4gb of ecs due to overallocation).
                let lambda_cost = TEST_MEM * 0.0000000167;
                let ecs_cost =
                    (1.0 * TEST_MEM * 0.012144 + 2.0 * TEST_MEM * 0.0013335) / (3600.0 * 1000.0);
                println!("Lambda cost: {lambda_cost}. ECS cost: {ecs_cost}");
                let cost_per_ms = if is_lambda { lambda_cost } else { ecs_cost };
                // Cost per ms.
                let cost_per_ms = if wait_time > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
                    let wait_time_ms = 1000.0 * (wait_time as f64);
                    if is_lambda {
                        cost_per_ms * total_duration_ms / (total_duration_ms + wait_time_ms)
                    } else {
                        cost_per_ms
                    }
                } else {
                    cost_per_ms
                };
                println!("Cost per ms: {cost_per_ms}");
                let duration = std::time::Duration::from_millis(duration_ms as u64);
                println!("Invoke took: {duration:?}");
                return (since, duration_ms, cost_per_ms);
            }
            Err(x) => {
                panic!("Error: {x:?}");
            }
        };
    }

    async fn run_invoke_test(variable: bool) {
        // Create benchmark object to initialize functions.
        let fc = FunctionalClient::new("microrunner").await;
        let _br = BenchRunner::new().await;
        let mut points = Vec::new();
        let start_time = std::time::Instant::now();
        let wait_time_secs: u64 = 20;
        if variable {
            // Low invoke rate.
            for _ in 0..10 {
                let point = do_invoke_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        // Sustained invokes.
        for _ in 0..200 {
            let point = do_invoke_round(&fc, start_time, 0).await;
            points.push(point);
        }
        if variable {
            // Low invoke rate.
            for _ in 0..10 {
                let point = do_invoke_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        if variable {
            write_output(points, "variable_invoke").await;
        } else {
            write_output(points, "invoke").await;
        }
    }

    #[tokio::test]
    async fn invoke_test() {
        run_invoke_test(false).await;
    }

    #[tokio::test]
    async fn variable_invoke_test() {
        run_invoke_test(true).await;
    }

    async fn do_messaging_round(
        fc: &FunctionalClient,
        start_time: std::time::Instant,
        wait_time: u64,
    ) -> (u64, f64, f64) {
        let mode_name = "message";
        let mode = serde_json::to_vec(mode_name).unwrap();
        let timestamp = std::time::Instant::now();
        let since = timestamp.duration_since(start_time).as_secs();
        let resp = fc.invoke_internal(&mode).await;
        println!("Invoke resp: {resp:?}");
        match resp {
            Ok((timing, _is_direct)) => {
                let (total_duration_ms, num_direct, num_indirect): (f64, f64, f64) =
                    serde_json::from_value(timing).unwrap();
                let duration_ms = total_duration_ms / (num_direct + num_indirect);
                // Compute total cost per active ms.
                // For simplification, assume all ecs if any ecs.
                let is_lambda = num_direct < 0.1;
                // Cost of every ms for 1gb lambda (0.5vcpu+1gb of ecs).
                let lambda_cost = TEST_MEM * 0.0000000167;
                let ecs_cost =
                    (0.5 * TEST_MEM * 0.012144 + TEST_MEM * 0.0013335) / (3600.0 * 1000.0);
                println!("LambdaCost={lambda_cost}; EcsCost={ecs_cost}");
                let cost_per_ms = if is_lambda { lambda_cost } else { ecs_cost };
                // Cost per ms.
                let cost_per_ms = if wait_time > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
                    let wait_time_ms = 1000.0 * (wait_time as f64);
                    if is_lambda {
                        cost_per_ms * total_duration_ms / (total_duration_ms + wait_time_ms)
                    } else {
                        cost_per_ms
                    }
                } else {
                    cost_per_ms
                };
                println!("Cost per ms: {cost_per_ms}");
                return (since, duration_ms, cost_per_ms);
            }
            Err(x) => {
                panic!("Error: {x:?}");
            }
        };
    }

    async fn run_message_test(variable: bool) {
        // Create benchmark object to initialize functions.
        let fc = FunctionalClient::new("microrunner").await;
        let _br = BenchRunner::new().await;
        let mut points: Vec<(u64, f64, f64)> = Vec::new();
        let start_time = std::time::Instant::now();
        let wait_time_secs: u64 = 30;
        if variable {
            // Low message rate.
            for _ in 0..10 {
                let point = do_messaging_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        // High message rate.
        for _ in 0..100 {
            let point = do_messaging_round(&fc, start_time, 0).await;
            points.push(point);
        }
        if variable {
            // Low message rate.
            for _ in 0..10 {
                let point = do_messaging_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        if variable {
            write_output(points, "variable_message").await;
        } else {
            write_output(points, "message").await;
        }
    }

    #[tokio::test]
    async fn message_test() {
        run_message_test(false).await;
    }

    #[tokio::test]
    async fn variable_message_test() {
        run_message_test(true).await;
    }

    async fn do_persist_round(
        fc: &FunctionalClient,
        start_time: std::time::Instant,
        wait_time: u64,
    ) -> (u64, f64, f64) {
        let mode_name = "persist";
        let mode = serde_json::to_vec(mode_name).unwrap();
        let timestamp = std::time::Instant::now();
        let since = timestamp.duration_since(start_time).as_secs();
        let resp = fc.invoke_internal(&mode).await;
        println!("Invoke resp: {resp:?}");
        if let Ok((timing, _)) = &resp {
            let total_duration_ms: f64 = serde_json::from_value(timing.clone()).unwrap();
            let duration_ms = total_duration_ms / 10.0;
            // Hacky way of getting commit mode.
            // TODO: Have something formal.
            let (is_lambda, is_replicated) = if total_duration_ms < 30.0 {
                println!("Was ecs + replication");
                (false, true)
            } else if total_duration_ms < 200.0 {
                println!("Was ecs");
                (false, false)
            } else {
                println!("Was lambda");
                (true, false)
            };
            // Cost of every ms per 1gb of lambda (1gb+0.5vcpu of ecs).
            let lambda_cost = 0.0000000167;
            let ecs_cost = (0.5 * 0.012144 + 1.0 * 0.0013335) / (3600.0 * 1000.0);
            let cost_per_ms = if is_lambda {
                // lambda.
                lambda_cost * TEST_MEM
            } else if !is_replicated {
                // ecs.
                ecs_cost * TEST_MEM
            } else {
                // ecs + six replica nodes.
                ecs_cost * TEST_MEM + ecs_cost * 2.0 * 6.0
            };
            // Cost per ms.
            let cost_per_ms = if wait_time > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
                let wait_time_ms = 1000.0 * (wait_time as f64);
                if is_lambda {
                    cost_per_ms * total_duration_ms / (total_duration_ms + wait_time_ms)
                } else {
                    cost_per_ms
                }
            } else {
                cost_per_ms
            };
            return (since, duration_ms, cost_per_ms);
        } else {
            panic!("Error: {resp:?}");
        }
    }

    async fn run_persist_test(variable: bool) {
        // Create benchmark object to initialize functions.
        let fc = FunctionalClient::new("microrunner").await;
        let _br = BenchRunner::new().await;
        let mut points: Vec<(u64, f64, f64)> = Vec::new();
        let start_time = std::time::Instant::now();
        let wait_time_secs: u64 = 30;
        if variable {
            // Low rate.
            for _ in 0..10 {
                let point = do_persist_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        // High rate.
        for _ in 0..500 {
            let point = do_persist_round(&fc, start_time, 0).await;
            points.push(point);
        }
        if variable {
            // Low rate.
            for _ in 0..10 {
                let point = do_persist_round(&fc, start_time, wait_time_secs).await;
                points.push(point);
            }
        }
        if variable {
            write_output(points, "variable_persist").await;
        } else {
            write_output(points, "persist").await;
        }
    }

    #[tokio::test]
    async fn persist_test() {
        run_persist_test(false).await;
    }

    #[tokio::test]
    async fn variable_persist_test() {
        run_persist_test(true).await;
    }
}
