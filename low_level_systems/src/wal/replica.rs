use crate::LLError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A replica.
#[derive(Clone)]
pub struct Replica {
    inner: Arc<RwLock<ReplicaInner>>,
    client: ureq::Agent,
}

/// Inner state of the replica.
pub struct ReplicaInner {
    highest_seen_incarnation: i64,
    entries: HashMap<usize, Vec<(Vec<u8>, i64)>>,
}

/// A replica request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicaReq {
    Primary {
        incarnation: i64,
        partition_idx: usize,
        secondary_url: String,
        entries: Vec<(Vec<u8>, i64)>,
    },
    Secondary {
        incarnation: i64,
        partition_idx: usize,
        entries: Vec<(Vec<u8>, i64)>,
    },
    RecoverPrimary {
        incarnation: i64,
        partition_idx: usize,
        secondary_url: String,
    },
    RecoverSecondary {
        incarnation: i64,
        partition_idx: usize,
    },
}

/// A replica response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaResp {
    /// The highest seen incarnation.
    pub highest_seen_incarnation: i64,
    /// Only populated for recovery requests.
    pub recovered_entries: Vec<(Vec<u8>, i64)>,
}

impl Replica {
    /// Create a new replica.
    pub fn new() -> Replica {
        let client = ureq::AgentBuilder::new()
            .timeout_read(std::time::Duration::from_secs(1))
            .timeout_connect(std::time::Duration::from_secs(1))
            .timeout_write(std::time::Duration::from_secs(1))
            .build();
        Replica {
            inner: Arc::new(RwLock::new(ReplicaInner {
                highest_seen_incarnation: 0,
                entries: HashMap::new(),
            })),
            client,
        }
    }

    /// Process a request.
    pub fn process_req(&self, req: ReplicaReq) -> ReplicaResp {
        match req {
            ReplicaReq::Primary {
                incarnation,
                partition_idx,
                secondary_url,
                entries,
            } => self.handle_primary_req(incarnation, partition_idx, entries, secondary_url),
            ReplicaReq::Secondary {
                incarnation,
                partition_idx,
                entries,
            } => self.handle_secondary_req(incarnation, partition_idx, entries),
            ReplicaReq::RecoverPrimary {
                incarnation,
                partition_idx,
                secondary_url,
            } => self.handle_recover_primary_req(incarnation, partition_idx, secondary_url),
            ReplicaReq::RecoverSecondary {
                incarnation,
                partition_idx,
            } => self.handle_recover_secondary_req(incarnation, partition_idx),
        }
    }

    /// Handle an HTTP request.
    fn handle_http_request(&self, mut request: tiny_http::Request) {
        let mut body = vec![];
        request.as_reader().read_to_end(&mut body).unwrap();
        let req = bincode::deserialize(&body).unwrap();
        let resp = self.process_req(req);
        let resp_meta = String::new();
        let resp_body = bincode::serialize(&resp).unwrap();
        let resp_body = resp_body.as_slice();
        let resp = tiny_http::Response::new(
            tiny_http::StatusCode(200),
            vec![
                tiny_http::Header {
                    field: "obelisk-meta".parse().unwrap(),
                    value: resp_meta.clone().parse().unwrap(),
                },
                tiny_http::Header {
                    field: "Content-Type".parse().unwrap(),
                    value: "application/octet-stream".parse().unwrap(),
                },
                tiny_http::Header {
                    field: "Content-Length".parse().unwrap(),
                    value: resp_body.len().to_string().parse().unwrap(),
                },
            ],
            resp_body,
            Some(resp_body.len()),
            None,
        );
        request.respond(resp).unwrap();
    }

    /// Launch a local server.
    pub fn launch_local_server(&self, port: u16) {
        let replica = self.clone();
        std::thread::spawn(move || {
            let addr = format!("0.0.0.0:{port}");
            let server = Arc::new(tiny_http::Server::http(addr).unwrap());
            let mut workers = vec![];
            println!("Server listening on port {port}!");
            for _ in 0..4 {
                let replica = replica.clone();
                let server = server.clone();
                workers.push(std::thread::spawn(move || {
                    for request in server.incoming_requests() {
                        // println!("Received request {port}!");
                        replica.handle_http_request(request);
                    }
                }));
            }
        });
    }

    /// Add entries to the replica.
    pub fn add_entries(
        &self,
        incarnation: i64,
        partition_idx: usize,
        entries: Vec<(Vec<u8>, i64)>,
    ) -> ReplicaResp {
        let mut inner = self.inner.write().unwrap();
        if incarnation < inner.highest_seen_incarnation {
            return ReplicaResp {
                highest_seen_incarnation: inner.highest_seen_incarnation,
                recovered_entries: vec![],
            };
        }
        inner.highest_seen_incarnation = incarnation;
        let replace_entries = if let Some(old_entries) = inner.entries.get(&partition_idx) {
            let old_last = old_entries.last().unwrap().1;
            let new_last = entries.last().unwrap().1;
            old_last < new_last
        } else {
            true
        };
        if replace_entries {
            inner.entries.insert(partition_idx, entries);
        }
        ReplicaResp {
            highest_seen_incarnation: inner.highest_seen_incarnation,
            recovered_entries: vec![],
        }
    }

    /// Send entries to the secondary.
    pub fn send_to_secondary(
        &self,
        incarnation: i64,
        partition_idx: usize,
        entries: Vec<(Vec<u8>, i64)>,
        secondary_url: String,
    ) -> Result<(), LLError> {
        let req = ReplicaReq::Secondary {
            incarnation,
            partition_idx,
            entries,
        };
        let body = bincode::serialize(&req).unwrap();
        let body = body.as_slice();
        let resp = self
            .client
            .post(&secondary_url)
            .set("obelisk-meta", "")
            .set("Content-Type", "application/octet-stream")
            .set("Content-Length", &body.len().to_string())
            .send(body)
            .map_err(|e| LLError::WALError(format!("Failed to send to secondary: {}", e)))?;
        if resp.status() != 200 {
            return Err(LLError::WALError(format!(
                "Failed to send to secondary: {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// Handle a primary request.
    pub fn handle_primary_req(
        &self,
        incarnation: i64,
        partition_idx: usize,
        entries: Vec<(Vec<u8>, i64)>,
        secondary_url: String,
    ) -> ReplicaResp {
        println!("Primary request: Incarnation={incarnation}. Partition={partition_idx}. Entries={len}. Secondary={secondary_url}", len=entries.len());
        let secondary_resp =
            self.send_to_secondary(incarnation, partition_idx, entries.clone(), secondary_url);
        if secondary_resp.is_err() {
            return ReplicaResp {
                highest_seen_incarnation: -1,
                recovered_entries: vec![],
            };
        }
        self.add_entries(incarnation, partition_idx, entries)
    }

    /// Handle a secondary request.
    pub fn handle_secondary_req(
        &self,
        incarnation: i64,
        partition_idx: usize,
        entries: Vec<(Vec<u8>, i64)>,
    ) -> ReplicaResp {
        println!("Secondary request: Incarnation={incarnation}. Partition={partition_idx}. Entries={len}.", len=entries.len());
        self.add_entries(incarnation, partition_idx, entries)
    }

    /// Recover entries.
    pub fn recover_entries(
        &self,
        incarnation: i64,
        partition_idx: usize,
        secondary_resp: Option<ReplicaResp>,
    ) -> ReplicaResp {
        let mut inner = self.inner.write().unwrap();
        // If outdated, just return the highest seen incarnation.
        if incarnation < inner.highest_seen_incarnation {
            return ReplicaResp {
                highest_seen_incarnation: inner.highest_seen_incarnation,
                recovered_entries: vec![],
            };
        }
        inner.highest_seen_incarnation = incarnation;
        // If secondary has a higher incarnation, return that.
        if let Some(secondary_resp) = &secondary_resp {
            if secondary_resp.highest_seen_incarnation > incarnation {
                inner.highest_seen_incarnation = secondary_resp.highest_seen_incarnation;
                return secondary_resp.clone();
            }
        }
        // If partition doesn't exist, create it.
        if !inner.entries.contains_key(&partition_idx) {
            inner.entries.insert(partition_idx, vec![]);
        }
        // Figure out if we should use the primary or secondary.
        let use_primary = if let Some(secondary_resp) = &secondary_resp {
            if secondary_resp.recovered_entries.len() == 0 {
                true
            } else if inner.entries.get(&partition_idx).unwrap().len() == 0 {
                false
            } else {
                let secondary_last = secondary_resp.recovered_entries.last().unwrap().1;
                let primary_last = inner.entries.get(&partition_idx).unwrap().last().unwrap().1;
                primary_last > secondary_last
            }
        } else {
            true
        };
        let entries = if use_primary {
            inner.entries.get(&partition_idx).unwrap().clone()
        } else {
            secondary_resp.unwrap().recovered_entries
        };
        ReplicaResp {
            highest_seen_incarnation: inner.highest_seen_incarnation,
            recovered_entries: entries,
        }
    }

    /// Recover from the secondary.
    pub fn recover_from_secondary(
        &self,
        incarnation: i64,
        partition_idx: usize,
        secondary_url: String,
    ) -> Result<ReplicaResp, LLError> {
        let req = ReplicaReq::RecoverSecondary {
            incarnation,
            partition_idx,
        };
        let body = bincode::serialize(&req).unwrap();
        let body = body.as_slice();
        // Increase retry?
        for _ in 0..5 {
            let resp = self
                .client
                .post(&secondary_url)
                .set("obelisk-meta", "")
                .set("Content-Type", "application/octet-stream")
                .set("Content-Length", &body.len().to_string())
                .send(body)
                .map_err(|e| LLError::WALError(format!("Failed to send to secondary: {}", e)));
            let resp = match resp {
                Ok(resp) => resp,
                Err(_) => continue,
            };
            if resp.status() != 200 {
                continue;
            }
            let mut body = vec![];
            resp.into_reader().read_to_end(&mut body).unwrap();
            let resp = bincode::deserialize(&body).unwrap();
            return Ok(resp);
        }
        Err(LLError::WALError(
            "Failed to recover from secondary".to_string(),
        ))
    }

    /// Handle a recover primary request.
    pub fn handle_recover_primary_req(
        &self,
        incarnation: i64,
        partition_idx: usize,
        secondary_url: String,
    ) -> ReplicaResp {
        let secondary_resp = self.recover_from_secondary(incarnation, partition_idx, secondary_url);
        self.recover_entries(incarnation, partition_idx, secondary_resp.ok())
    }

    /// Handle a recover secondary request.
    pub fn handle_recover_secondary_req(
        &self,
        incarnation: i64,
        partition_idx: usize,
    ) -> ReplicaResp {
        self.recover_entries(incarnation, partition_idx, None)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::vec;

    use super::*;
    use nix::sys::{signal, wait};
    use nix::unistd;

    fn send_local_request(req: ReplicaReq, port: u16) -> ReplicaResp {
        let body = bincode::serialize(&req).unwrap();
        let body = body.as_slice();
        let resp = ureq::post(&format!("http://localhost:{port}/"))
            .set("obelisk-meta", "")
            .set("Content-Type", "application/octet-stream")
            .set("Content-Length", &body.len().to_string())
            .send(body)
            .unwrap();
        let mut body = vec![];
        resp.into_reader().read_to_end(&mut body).unwrap();
        let resp = bincode::deserialize(&body).unwrap();
        resp
    }

    #[test]
    fn basic_test_replica() {
        let primary_port = 37000;
        let secondary_port = 37001;
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Child) => {
                let primary = Replica::new();
                primary.launch_local_server(primary_port);
                let secondary = Replica::new();
                secondary.launch_local_server(secondary_port);
                std::thread::sleep(std::time::Duration::from_secs(30));
                std::process::exit(0);
            }
            Ok(unistd::ForkResult::Parent { child }) => {
                // Wait for servers to start.
                println!("Waiting for servers to start...");
                std::thread::sleep(std::time::Duration::from_secs(5));
                // Send entries.
                let req = ReplicaReq::Primary {
                    incarnation: 1,
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                    entries: vec![(vec![1, 2, 3], 1), (vec![4, 5, 6], 2)],
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                assert_eq!(resp.recovered_entries.len(), 0);
                // Recover from primary.
                let req = ReplicaReq::RecoverPrimary {
                    incarnation: 1,
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                assert_eq!(resp.recovered_entries.len(), 2);
                assert_eq!(resp.recovered_entries[0].1, 1);
                assert_eq!(resp.recovered_entries[1].1, 2);
                // Recover from secondary.
                let req = ReplicaReq::RecoverSecondary {
                    incarnation: 1,
                    partition_idx: 0,
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                assert_eq!(resp.recovered_entries.len(), 2);
                assert_eq!(resp.recovered_entries[0].1, 1);
                assert_eq!(resp.recovered_entries[1].1, 2);
                // Send outdated incarnation.
                let req = ReplicaReq::Primary {
                    incarnation: 0, // Outdated
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                    entries: vec![(vec![1, 2, 3], 3), (vec![4, 5, 6], 4)],
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                // Send outdated entries.
                let req = ReplicaReq::Primary {
                    incarnation: 1,
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                    entries: vec![(vec![1, 2, 3], 0)], // Outdated
                };
                let _resp = send_local_request(req, primary_port);
                // Previous requests should be ignored.
                // Re-recover from primary.
                let req = ReplicaReq::RecoverPrimary {
                    incarnation: 1,
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                assert_eq!(resp.recovered_entries.len(), 2);
                assert_eq!(resp.recovered_entries[0].1, 1);
                assert_eq!(resp.recovered_entries[1].1, 2);
                // Re-recover from secondary.
                let req = ReplicaReq::RecoverSecondary {
                    incarnation: 1,
                    partition_idx: 0,
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 1);
                assert_eq!(resp.recovered_entries.len(), 2);
                assert_eq!(resp.recovered_entries[0].1, 1);
                assert_eq!(resp.recovered_entries[1].1, 2);
                // Done.
                signal::kill(child, signal::SIGKILL).unwrap();
                // Wait
                let res = wait::waitpid(child, None).unwrap();
                println!("Wait result: {:?}", res);
            }
            Err(_) => panic!("Fork failed"),
        }
    }

    use crate::wal::*;
    const STORAGE_DIR: &str = "storage";

    /// Reset test.
    fn reset_test(storage_dir: &str, name: &str) {
        std::env::set_var("RUST_LOG", "info");
        env_logger::builder()
            .format_timestamp(None)
            .format_level(true)
            .format_module_path(false)
            .format_target(false)
            .try_init()
            .unwrap_or(());
        reset_wal(storage_dir, name);
    }

    /// Replay all entries from the manager.
    fn replay_all(manager: &manager::Manager) -> Vec<(Vec<u8>, i64)> {
        let mut entries = Vec::new();
        let mut handle = None;
        loop {
            let (new_handle, new_entries) = manager.replay(handle).unwrap();
            if let Some(new_handle) = new_handle {
                // println!("Replaying entries: {new_entries:?}");
                handle = Some(new_handle);
                entries.extend(new_entries);
            } else {
                break;
            }
        }
        entries
    }

    /// Multi-threaded check that enqueue/persist/replay/truncate works even with multiple slabs.
    fn do_multi_threaded_multi_slab_test(num_slabs: usize, entry_size: usize, parallelism: usize) {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        let mut entries = Vec::new();
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            manager
                .set_replicas(Some((
                    "http://localhost:37000/".to_string(),
                    "http://localhost:37001/".to_string(),
                )))
                .unwrap();
            let manager = Arc::new(manager);
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                threads.push(std::thread::spawn(move || {
                    let mut entries = Vec::new();
                    let start_idx = p * (num_entries / parallelism);
                    let end_idx = (p + 1) * (num_entries / parallelism);
                    for i in start_idx..end_idx {
                        let val = (i % 256) as u8;
                        let entry: Vec<u8> = vec![val; entry_size];
                        let lsn = manager.enqueue(entry.clone()).unwrap();
                        entries.push((entry, lsn));
                        // Persist every 10 entries.
                        if i > 0 && i % 10 == 0 {
                            manager.persist(None).unwrap();
                        }
                    }
                    // Persist the potentially remaining entries.
                    manager.persist(None).unwrap();
                    entries
                }));
            }
            for t in threads {
                let res = t.join().unwrap();
                entries.extend(res);
            }
        }
        // Sort entries by lsn.
        entries.sort_by_key(|e| e.1);
        // Read and check, then truncate.
        let truncate_idx = {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            let found_entries = replay_all(&manager);
            assert_eq!(found_entries.len(), entries.len());
            for i in 0..num_entries {
                assert_eq!(entries[i].0, found_entries[i].0);
                assert_eq!(entries[i].1, found_entries[i].1);
            }
            // Now truncate half of the entries.
            let truncate_idx = entries.len() / 2;
            assert!(truncate_idx > 0);
            let truncate_lsn = entries[truncate_idx - 1].1;
            manager.truncate(truncate_lsn).unwrap();
            truncate_idx
        };
        // Check, then insert some more entries to make sure that slab reuse works.
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            let found_entries = replay_all(&manager);
            assert_eq!(found_entries.len(), entries.len() - truncate_idx);
            for i in 0..found_entries.len() {
                let idx = i + truncate_idx;
                assert_eq!(entries[idx].0, found_entries[i].0);
                assert_eq!(entries[idx].1, found_entries[i].1);
            }
            // Insert more entries.
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                threads.push(std::thread::spawn(move || {
                    let mut entries = Vec::new();
                    let start_idx = p * (num_entries / parallelism);
                    let end_idx = (p + 1) * (num_entries / parallelism);
                    for i in start_idx..end_idx {
                        let val = (i % 256) as u8;
                        let entry: Vec<u8> = vec![val; entry_size];
                        let lsn = manager.enqueue(entry.clone()).unwrap();
                        entries.push((entry, lsn));
                        // Persist every 10 entries.
                        if i > 0 && i % 10 == 0 {
                            manager.persist(None).unwrap();
                        }
                    }
                    // Persist the potentially remaining entries.
                    manager.persist(None).unwrap();
                    entries
                }));
            }
            for t in threads {
                let res = t.join().unwrap();
                entries.extend(res);
            }
        }
        // Sort entries by lsn.
        entries.sort_by_key(|e| e.1);
        // Now replay all
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            let found_entries = replay_all(&manager);
            assert_eq!(found_entries.len(), entries.len() - truncate_idx);
            for i in 0..found_entries.len() {
                let idx = i + truncate_idx;
                assert_eq!(entries[idx].0, found_entries[i].0);
                assert_eq!(entries[idx].1, found_entries[i].1);
            }
        }
        let final_entries = entries[truncate_idx..].to_vec();
        let entries = bincode::serialize(&final_entries).unwrap();
        {
            let mut file =
                std::fs::File::create(format!("{STORAGE_DIR}/{name}.ref").as_str()).unwrap();
            file.write_all(entries.as_slice()).unwrap();
        }
    }

    /// Multi-threaded test where some persist fails in the middle.
    /// Check that subsequent persists fail and can be undone.
    fn do_failure_injection_test(
        num_slabs: usize,
        entry_size: usize,
        parallelism: usize,
        sleep_time: Option<u64>,
    ) {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        let mut entries = Vec::new();
        let manager = {
            let manager =
                manager::Manager::new_with_incarnation(STORAGE_DIR, name, parallelism, 0).unwrap();
            manager
                .set_replicas(Some((
                    "http://localhost:37000/".to_string(),
                    "http://localhost:37001/".to_string(),
                )))
                .unwrap();
            let manager = Arc::new(manager);
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                threads.push(std::thread::spawn(move || {
                    let mut entries = Vec::new();
                    let start_idx = p * (num_entries / parallelism);
                    let end_idx = (p + 1) * (num_entries / parallelism);
                    for i in start_idx..end_idx {
                        let val = (i % 256) as u8;
                        let entry: Vec<u8> = vec![val; entry_size];
                        let lsn = manager.enqueue(entry.clone()).unwrap();
                        entries.push((entry, lsn));
                        // Persist every 10 entries.
                        if i > 0 && i % 10 == 0 {
                            manager.persist(None).unwrap();
                        }
                    }
                    // Persist the potentially remaining entries.
                    manager.persist(None).unwrap();
                    entries
                }));
            }
            for t in threads {
                let res = t.join().unwrap();
                entries.extend(res);
            }
            manager
        };
        // When sleeping, the incarnation number changes before entries are appended.
        let should_append_extra_entries = sleep_time.is_none();
        // Sort entries by lsn.
        entries.sort_by_key(|e| e.1);
        if should_append_extra_entries {
            // Complete reference vector.
            let hi_lsn = entries.last().unwrap().1;
            for p in 0..parallelism {
                let entry = vec![p as u8; 1024];
                entries.push((entry, hi_lsn + 1 + p as i64));
            }
        }
        // Write to storage.
        let entries = bincode::serialize(&entries).unwrap();
        {
            let mut file =
                std::fs::File::create(format!("{STORAGE_DIR}/{name}.ref").as_str()).unwrap();
            file.write_all(entries.as_slice()).unwrap();
        }
        if let Some(sleep_time) = sleep_time {
            std::thread::sleep(std::time::Duration::from_secs(sleep_time));
        }
        // Now inject a disk failure in the first thread.
        // The replica succeeds, which matches the reference above.
        // Afterwards, the process will crash.
        {
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                threads.push(std::thread::spawn(move || {
                    // Only the first will succeed, the second should block.
                    for _ in 0..2 {
                        // Wait a different time in each thread to guarantee enqueue order in reference vector.
                        std::thread::sleep(std::time::Duration::from_millis(p as u64 * 50));
                        let lsn = manager.enqueue(vec![p as u8; 1024]).unwrap();
                        // Inject a disk failure
                        let res = manager.controlled_persist(None, false, true, None);
                        log::info!("Persisted entry with lsn {lsn}. P={p}.");
                        assert!(matches!(res, Ok(_)));
                        std::thread::sleep(std::time::Duration::from_millis(
                            (1 + parallelism as u64) * 50,
                        ));
                    }
                }));
            }
            for t in threads {
                let _ = t.join();
            }
        }
        // Should crash before this.
        std::process::exit(0);
    }

    fn compare_previous_entries(parallelism: usize, incarnation: i64) {
        let name = "essai";
        let manager =
            manager::Manager::new_with_incarnation(STORAGE_DIR, name, parallelism, incarnation)
                .unwrap();
        let current_entries = replay_all(&manager);
        let prev_entries: Vec<(Vec<u8>, i64)> = {
            let mut file =
                std::fs::File::open(format!("{STORAGE_DIR}/{name}.ref").as_str()).unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();
            bincode::deserialize(&buf).unwrap()
        };
        assert_eq!(current_entries.len(), prev_entries.len());
        for i in 0..current_entries.len() {
            assert_eq!(current_entries[i].1, prev_entries[i].1);
            assert_eq!(current_entries[i].0, prev_entries[i].0);
        }
    }

    #[test]
    fn test_replication_no_failures() {
        reset_test(STORAGE_DIR, "essai");
        let parallelism = 4;
        let primary_port = 37000;
        let secondary_port = 37001;
        let primary = Replica::new();
        primary.launch_local_server(primary_port);
        let secondary = Replica::new();
        secondary.launch_local_server(secondary_port);
        std::thread::sleep(std::time::Duration::from_secs(1));
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Child) => {
                let num_slabs = parallelism * partition::REPLAY_SLAB_COUNT * 5;
                let entry_size = 1024;
                do_multi_threaded_multi_slab_test(num_slabs, entry_size, parallelism);
                std::process::exit(0);
            }
            Ok(unistd::ForkResult::Parent { child }) => {
                let res = wait::waitpid(child, None).unwrap();
                // Assert ok.
                assert_eq!(res, wait::WaitStatus::Exited(child, 0));
                // Check that the previous entries are still there.
                compare_previous_entries(parallelism, 1);
            }
            Err(_) => panic!("Fork failed"),
        }
    }

    #[test]
    fn test_replication_disk_failures() {
        reset_test(STORAGE_DIR, "essai");
        let parallelism = 4;
        let primary_port = 37000;
        let secondary_port = 37001;
        let primary = Replica::new();
        primary.launch_local_server(primary_port);
        let secondary = Replica::new();
        secondary.launch_local_server(secondary_port);
        std::thread::sleep(std::time::Duration::from_secs(1));
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Child) => {
                let num_slabs = parallelism * partition::REPLAY_SLAB_COUNT * 5;
                let entry_size = 1024;
                do_failure_injection_test(num_slabs, entry_size, parallelism, None);
                std::process::exit(0);
            }
            Ok(unistd::ForkResult::Parent { child }) => {
                // Child should exit with an error due to disk failure.
                let res = wait::waitpid(child, None).unwrap();
                assert_eq!(res, wait::WaitStatus::Exited(child, 1));
                // Check that the previous entries are still there.
                compare_previous_entries(parallelism, 1);
            }
            Err(_) => panic!("Fork failed"),
        }
    }

    #[test]
    fn test_incarnation_change() {
        reset_test(STORAGE_DIR, "essai");
        let parallelism = 1;
        let primary_port = 37000;
        let secondary_port = 37001;
        let primary = Replica::new();
        primary.launch_local_server(primary_port);
        let secondary = Replica::new();
        secondary.launch_local_server(secondary_port);
        std::thread::sleep(std::time::Duration::from_secs(1));
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Child) => {
                let num_slabs = parallelism; // Keep small
                let entry_size = 1024;
                do_failure_injection_test(num_slabs, entry_size, parallelism, Some(30));
                std::process::exit(0);
            }
            Ok(unistd::ForkResult::Parent { child }) => {
                // Wait for child to finish first round of operations.
                std::thread::sleep(std::time::Duration::from_secs(15));
                // Now force a new incarnation.
                let req = ReplicaReq::RecoverPrimary {
                    incarnation: 2,
                    partition_idx: 0,
                    secondary_url: format!("http://localhost:{}/", secondary_port),
                };
                let resp = send_local_request(req, primary_port);
                assert_eq!(resp.highest_seen_incarnation, 2);
                // Child should exit with an error due to lost incarnation.
                let res = wait::waitpid(child, None).unwrap();
                assert_eq!(res, wait::WaitStatus::Exited(child, 1));
                // Check that the previous entries are still there.
                compare_previous_entries(parallelism, 2);
            }
            Err(_) => panic!("Fork failed"),
        }
    }
}
