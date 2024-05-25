use common::{HandlerKit, ScalingState, ServerlessHandler};
use low_level_systems::btree;
use persistence::PersistentLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const BTREE_PAGE_SIZE: usize = 4096;
const BTREE_PARALLELISM: usize = 8;

/// Request.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MicroKVReq {
    Get {
        key: String,
        caller_mem: i32,
    },
    Put {
        key: String,
        val: String,
        caller_mem: i32,
    },
    Delete {
        key: String,
        caller_mem: i32,
    },
}

/// Response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MicroKVResp {
    pub val: Option<Vec<u8>>,
}

/// Actor.
pub struct MicroKVActor {
    plog: Arc<PersistentLog>,
    metadata: String,
    btree: Arc<btree::Manager>,
}

/// Implementation.
impl MicroKVActor {
    /// Create actor.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            incarnation,
        } = kit;
        println!("Making micro bench actor: {}!", instance_info.identifier);
        let plog = Arc::new(
            PersistentLog::new(instance_info.clone(), incarnation)
                .await
                .unwrap(),
        );
        let name = instance_info.identifier.clone();
        let storage_dir = common::storage_path(&instance_info);
        let btree = Arc::new(
            btree::Manager::new_with_provider(
                BTREE_PARALLELISM,
                BTREE_PAGE_SIZE,
                &storage_dir,
                &name,
                plog.clone(),
            )
            .unwrap(),
        );
        // Avoid early return (meaning, lots of asynchronous background work) in Lambda.
        let setup = btree::manager::Setup {
            enable_early_return: instance_info.private_url.is_some(),
            enable_completion: true,
            enable_keep_pages: true,
        };
        btree.change_setup(setup);
        let metadata = {
            let is_lambda = instance_info.private_url.is_none();
            let memory = instance_info.mem;
            let metadata = (memory, is_lambda);
            serde_json::to_string(&metadata).unwrap()
        };
        let actor = MicroKVActor {
            plog,
            metadata,
            btree,
        };
        actor
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for MicroKVActor {
    /// Handle a message.
    async fn handle(&self, _msg: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: MicroKVReq = bincode::deserialize(&payload).unwrap();
        let resp: MicroKVResp = tokio::task::block_in_place(|| match req {
            MicroKVReq::Get { key, caller_mem } => {
                self.plog.set_default_caller_mem(caller_mem);
                let val = tokio::task::block_in_place(|| self.btree.get(key.as_bytes()).unwrap());
                MicroKVResp { val }
            }
            MicroKVReq::Put {
                key,
                val,
                caller_mem,
            } => {
                self.plog.set_default_caller_mem(caller_mem);
                tokio::task::block_in_place(|| {
                    self.btree.insert(key.as_bytes(), val.as_bytes()).unwrap()
                });
                MicroKVResp { val: None }
            }
            MicroKVReq::Delete { key, caller_mem } => {
                self.plog.set_default_caller_mem(caller_mem);
                tokio::task::block_in_place(|| self.btree.delete(key.as_bytes()).unwrap());
                MicroKVResp { val: None }
            }
        });
        let resp = bincode::serialize(&resp).unwrap();
        (self.metadata.clone(), resp)
    }

    /// Checkpoint. Just truncate the log up to and excluding the last entry.
    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        println!("Checkpoint: Terminating=({terminating})");
        self.plog.check_replicas(terminating).await;
    }
}

#[cfg(test)]
mod test {
    use functional::FunctionalClient;
    use rand::{distributions::Alphanumeric, Rng};
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::runner::RunnerReq;

    use super::*;
    use common::{InstanceInfo, ServerlessHandler};
    use rand::distributions::Distribution;

    const BENCH_KEY_SIZE: usize = 100;
    const BENCH_VAL_SIZE: usize = 900;
    const BENCH_DATA_SIZE_GB: usize = 20;
    // Not using powers of 2 for simplicity.
    const BENCH_NUM_KEYS: usize =
        (BENCH_DATA_SIZE_GB * 1000 * 1000 * 1000) / (BENCH_KEY_SIZE + BENCH_VAL_SIZE);
    const WRITE_RATE: f64 = 0.2;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_test() {
        run_basic_test().await;
    }

    async fn run_basic_test() {
        let caller_mem = 512;
        let fc = Arc::new(FunctionalClient::new("microbench", "microkv", Some(0), Some(512)).await);
        let req = MicroKVReq::Put {
            key: "Hello".into(),
            val: "World".into(),
            caller_mem,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = fc.invoke("", &req).await.unwrap();
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        println!("Put Resp: {resp:?}");
        let req = MicroKVReq::Get {
            key: "Hello".into(),
            caller_mem,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = fc.invoke("", &req).await.unwrap();
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        let resp = resp.val.map(|v| String::from_utf8(v).unwrap());
        println!("Get after insert Resp: {resp:?}");
        let req = MicroKVReq::Delete {
            key: "Hello".into(),
            caller_mem,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = fc.invoke("", &req).await.unwrap();
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        println!("Delete Resp: {resp:?}");
        let req = MicroKVReq::Get {
            key: "Hello".into(),
            caller_mem,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = fc.invoke("", &req).await.unwrap();
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        let resp = resp.val.map(|v| String::from_utf8(v).unwrap());
        println!("Get after delete Resp: {resp:?}");
    }

    async fn basic_populate(fc: &FunctionalClient, num_keys: usize) {
        for i in 0..num_keys {
            let key = format!("Hello{i}");
            let val = format!("World{i}");
            let req = MicroKVReq::Put {
                key,
                val,
                caller_mem: 512,
            };
            let req = bincode::serialize(&req).unwrap();
            let _ = fc.invoke("", &req).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_scaling_test() {
        run_basic_scaling_test(false, false).await;
    }

    async fn run_basic_scaling_test(read: bool, populate: bool) {
        let mem = 32768; // Large memory.
        let fc = Arc::new(FunctionalClient::new("microbench", "microkv", Some(0), Some(mem)).await);
        let num_keys = 10;
        if populate {
            basic_populate(&fc, num_keys).await;
        }
        let num_ops = if read { 3000 } else { 6000 };
        for n in 0..num_ops {
            let k = n % num_keys;
            let v = chrono::Utc::now().timestamp_micros() as usize % 1000;
            let key = format!("Hello{k}");
            let req = if read {
                MicroKVReq::Get {
                    key,
                    caller_mem: 512,
                }
            } else {
                MicroKVReq::Put {
                    key,
                    val: format!("World{v}"),
                    caller_mem: 512,
                }
            };
            let req = bincode::serialize(&req).unwrap();
            let start_time = std::time::Instant::now();
            let resp = fc.invoke("", &req).await;
            match resp {
                Ok((_, resp)) => {
                    let elapsed = start_time.elapsed();
                    let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
                    let resp = resp.val.map(|v| String::from_utf8(v).unwrap());
                    println!("Resp: {resp:?}. Elapsed: {elapsed:?}");
                }
                Err(e) => {
                    println!("Error: {e}");
                    return;
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_test_local() {
        run_basic_test_local().await;
    }

    async fn run_basic_test_local() {
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        std::env::set_var("OBK_EXTERNAL_ACCESS", false.to_string());
        let instance_info = Arc::new(InstanceInfo {
            peer_id: "444-555-666".into(),
            az: None,
            mem: 1769,
            cpus: 1024,
            public_url: Some("chezmoi.com".into()),
            private_url: None,
            service_name: None,
            handler_name: Some("microkv".into()),
            subsystem: "functional".into(),
            namespace: "wal".into(),
            identifier: "microkv0".into(),
            unique: true,
            persistent: true,
        });
        let kit = common::HandlerKit {
            instance_info: instance_info.clone(),
            incarnation: 0,
        };
        let actor = super::MicroKVActor::new(kit).await;
        let req = MicroKVReq::Put {
            key: "Hello".into(),
            val: "World".into(),
            caller_mem: 512,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = actor.handle("".into(), req).await;
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        println!("Put Resp: {resp:?}");
        let req = MicroKVReq::Get {
            key: "Hello".into(),
            caller_mem: 512,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = actor.handle("".into(), req).await;
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        println!(
            "Get Resp: {resp:?}",
            resp = String::from_utf8(resp.val.unwrap()).unwrap()
        );
    }

    fn make_kv(k: usize) -> (Vec<u8>, Vec<u8>) {
        let mut h = DefaultHasher::new();
        k.hash(&mut h);
        let key = h.finish();
        let key = format!("{key:16x}", key = key).as_bytes().to_vec();
        let key = key.iter().cycle().take(BENCH_KEY_SIZE).cloned().collect();
        let val = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(BENCH_VAL_SIZE)
            .collect();
        (key, val)
    }

    fn make_kv_str(k: usize) -> (String, String) {
        let (key, val) = make_kv(k);
        (
            String::from_utf8(key).unwrap(),
            String::from_utf8(val).unwrap(),
        )
    }

    #[test]
    fn generate_local_data() {
        let num_keys = BENCH_NUM_KEYS;
        println!("Generating {num_keys} keys.");
        std::env::set_var("OBK_MEMORY", "4096");
        let storage_dir = common::shared_storage_prefix();
        let name = "microkv0".to_string();
        let namespace = "microbench".to_string();
        let storage_dir = format!("{storage_dir}/{namespace}/{name}");
        let _ = std::fs::create_dir_all(&storage_dir);
        let btree =
            btree::Manager::new(BTREE_PARALLELISM, BTREE_PAGE_SIZE, &storage_dir, &name).unwrap();
        let setup = btree::manager::Setup {
            enable_early_return: false,
            enable_completion: true,
            enable_keep_pages: true,
        };
        btree.change_setup(setup);
        let btree = Arc::new(btree);
        let start_time = std::time::Instant::now();
        let num_threads = 4;
        let mut ts = vec![];
        for i in 0..num_threads {
            let btree = btree.clone();
            let start = i * num_keys / num_threads;
            let end = (i + 1) * num_keys / num_threads;
            ts.push(std::thread::spawn(move || {
                let thread_idx = i;
                for i in start..end {
                    let so_far = i - start;
                    let (key, val) = make_kv(i);
                    btree.insert(&key, &val).unwrap();
                    if thread_idx == 0 && i % 10000 == 0 {
                        btree.perform_checkpoint();
                    }
                    if i % 5000 == 0 {
                        let total = end - start;
                        println!("Thread {thread_idx}. Inserted {so_far}/{total} keys.");
                    }
                }
            }));
        }
        ts.into_iter().for_each(|t| t.join().unwrap());
        let elapsed = start_time.elapsed();
        println!("Elapsed: {elapsed:?}");
    }

    #[test]
    fn read_local_data() {
        std::env::set_var("OBK_MEMORY", "4096");
        let storage_dir = common::shared_storage_prefix();
        let name = "microkv0".to_string();
        let namespace = "microbench".to_string();
        let storage_dir = format!("{storage_dir}/{namespace}/{name}");
        let _ = std::fs::create_dir_all(&storage_dir);
        let btree =
            btree::Manager::new(BTREE_PARALLELISM, BTREE_PAGE_SIZE, &storage_dir, &name).unwrap();
        let ks = [0, BENCH_NUM_KEYS / 2, BENCH_NUM_KEYS - 1];
        for k in ks {
            let (key, _) = make_kv(k);
            let val = btree.get(&key).unwrap();
            println!(
                "Key: {key_len:?}. Val: {val_len:?}",
                key_len = key.len(),
                val_len = val.map(|v| v.len())
            );
        }
        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn read_loaded_data() {
        run_read_loaded_data().await;
    }

    async fn run_read_loaded_data() {
        let caller_mem = 512;
        let fc = Arc::new(FunctionalClient::new("microbench", "microkv", Some(0), Some(512)).await);
        let (key, _value) = make_kv_str(0);
        let req = MicroKVReq::Get {
            key: key.clone(),
            caller_mem,
        };
        let req = bincode::serialize(&req).unwrap();
        let (_, resp) = fc.invoke("", &req).await.unwrap();
        let resp = bincode::deserialize::<MicroKVResp>(&resp).unwrap();
        let val = resp.val.map(|v| String::from_utf8(v).unwrap());
        println!("Get Resp. Key={key}. Val={val:?}");
    }

    struct RequestSender {
        curr_avg_latency: f64,
        desired_requests_per_second: f64,
        thread_cap: f64,
        fc: Arc<FunctionalClient>,
    }

    impl RequestSender {
        // Next 5 seconds of requests
        async fn send_request_window(&mut self) -> Vec<(Duration, i32, bool, MicroKVResp)> {
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
                let t = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let mut responses = Vec::new();
                    let mut curr_idx = 0;
                    while curr_idx < requests_per_thread {
                        // Find number of calls to make and update curr idx.
                        let batch_size = 50;
                        let call_count = if requests_per_thread - curr_idx < batch_size {
                            requests_per_thread - curr_idx
                        } else {
                            batch_size
                        };
                        curr_idx += call_count;
                        // Now send requests.
                        let reqs = {
                            let dist = zipf::ZipfDistribution::new(BENCH_NUM_KEYS, 2.0).unwrap();
                            let mut rng = rand::thread_rng();
                            let ks = (0..call_count)
                                .map(|_| dist.sample(&mut rng))
                                .collect::<Vec<usize>>();
                            let reqs = ks
                                .into_iter()
                                .map(|k| {
                                    let is_write = rng.gen_bool(WRITE_RATE);
                                    let (key, val) = make_kv_str(k);
                                    if is_write {
                                        MicroKVReq::Put {
                                            key,
                                            val,
                                            caller_mem: 512,
                                        }
                                    } else {
                                        MicroKVReq::Get {
                                            key,
                                            caller_mem: 512,
                                        }
                                    }
                                })
                                .collect::<Vec<MicroKVReq>>();
                            let reqs = RunnerReq::KV(reqs);
                            bincode::serialize(&reqs).unwrap()
                        };
                        let resp = fc.invoke("", &reqs).await;
                        if resp.is_err() {
                            println!("Err: {resp:?}");
                            continue;
                        }
                        let (_, resp) = resp.unwrap();
                        let mut resp: Vec<(Duration, i32, bool, MicroKVResp)> =
                            bincode::deserialize(&resp).unwrap();
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
    async fn write_bench_output(points: Vec<(u64, f64, i32, bool, bool)>, expt_name: &str) {
        let expt_dir = "results/microkv";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, duration, mem, is_lambda, is_write) in points {
            let mode = if is_lambda { "Lambda" } else { "ECS" };
            let op = if is_write { "Write" } else { "Read" };
            writer
                .write_record(&[
                    since.to_string(),
                    duration.to_string(),
                    mem.to_string(),
                    mode.to_string(),
                    op.to_string(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    async fn run_bench(request_sender: &mut RequestSender, name: &str, test_duration: Duration) {
        let mut results = Vec::new();
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
            let resp = request_sender.send_request_window().await;
            for (duration, mem, is_lambda, resp) in &resp {
                let is_write = resp.val.is_none();
                results.push((since, duration.as_secs_f64(), *mem, *is_lambda, is_write));
            }
        }
        write_bench_output(results, name).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_fn_bench_cloud() {
        let fc =
            Arc::new(FunctionalClient::new("microbench", "microrunner", None, Some(512)).await);
        let duration_mins = 5.0;
        let _low_req_per_secs = 4.0;
        let _medium_req_per_secs = 40.0;
        let _high_req_per_secs = 400.0;
        let mut request_sender = RequestSender {
            curr_avg_latency: 0.100,
            desired_requests_per_second: 0.0,
            fc: fc.clone(),
            thread_cap: 100.0,
        };
        // // Low
        // request_sender.desired_requests_per_second = _low_req_per_secs;
        // request_sender.thread_cap = 1.0 + 1e-4;
        // run_bench(
        //     &mut request_sender,
        //     "pre_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // )
        // .await;
        // Medium
        request_sender.desired_requests_per_second = _medium_req_per_secs;
        request_sender.thread_cap = 1.0 + 1e-4;
        run_bench(
            &mut request_sender,
            "pre_medium",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // High
        request_sender.desired_requests_per_second = _high_req_per_secs;
        request_sender.thread_cap = 10.0 + 1e-4;
        run_bench(
            &mut request_sender,
            "pre_high",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // // Low again
        // request_sender.desired_requests_per_second = _low_req_per_secs;
        // request_sender.thread_cap = 1.0 + 1e-4;
        // run_bench(
        //     &mut request_sender,
        //     "post_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // ).await;
    }
}
