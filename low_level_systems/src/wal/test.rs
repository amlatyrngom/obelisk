#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    /// Check that single enqueue/persist/replay works.
    #[test]
    fn basic_test() {
        let parallelism = 1;
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let (entry, lsn) = {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            let entry: Vec<u8> = vec![37; 1024];
            let lsn = manager.enqueue(entry.clone()).unwrap();
            println!("lsn: {}", lsn);
            manager.persist(None).unwrap();
            (entry, lsn)
        };
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            assert_eq!(manager.persist_lsn(), lsn);
            let entries = replay_all(&manager);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, entry);
            assert_eq!(entries[0].1, lsn);
        }
        reset_test(STORAGE_DIR, name);
    }

    /// Single-threaded check that enqueue/persist/replay/truncate works even with multiple slabs.
    fn do_multi_slab_test(num_slabs: usize, entry_size: usize, parallelism: usize) {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        // Insert entries.
        let mut entries = Vec::new();
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            for i in 0..num_entries {
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
        }
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
            for i in 0..num_entries {
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
        }
        // Now replay all to check.
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
    }

    /// Multi-threaded check that enqueue/persist/replay/truncate works even with multiple slabs.
    fn do_multi_threaded_multi_slab_test(num_slabs: usize, entry_size: usize, parallelism: usize) {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        let mut entries = Vec::new();
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
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
    }

    /// Multi-threaded test where some persist fails in the middle.
    /// Check that subsequent persists fail and can be undone.
    fn do_failure_injection_test(num_slabs: usize, entry_size: usize, parallelism: usize) {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        let mut entries = Vec::new();
        let manager = {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
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
        // Sort entries by lsn.
        entries.sort_by_key(|e| e.1);
        // Now inject a failure in the first thread.
        {
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                threads.push(std::thread::spawn(move || {
                    if p == 0 {
                        // Inject a failure.
                        manager.enqueue(vec![p as u8; 1024]).unwrap();
                        let res = manager.controlled_persist(None, true, false, None);
                        assert!(matches!(res, Err(LLError::WALError(_))));
                    } else {
                        // Wait for the failure to be injected.
                        // Wait a different time in each thread to guarantee enqueue/persist order.
                        std::thread::sleep(std::time::Duration::from_millis(p as u64 * 50));
                        // Now persist. Should fail since the first thread failed.
                        manager.enqueue(vec![p as u8; 1024]).unwrap();
                        let res = manager.controlled_persist(None, false, false, None);
                        assert!(matches!(res, Err(LLError::WALError(_))));
                    }
                }));
            }
            for t in threads {
                let _ = t.join();
            }
        }
        drop(manager);
        // Replay all. Failed persist should not be found.
        // Then insert more to make sure that the failed persist did not corrupt the log.
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            let found_entries = replay_all(&manager);
            assert_eq!(found_entries.len(), entries.len());
            for i in 0..found_entries.len() {
                assert_eq!(entries[i].0, found_entries[i].0);
                assert_eq!(entries[i].1, found_entries[i].1);
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
            assert_eq!(found_entries.len(), entries.len());
            for i in 0..found_entries.len() {
                assert_eq!(entries[i].0, found_entries[i].0);
                assert_eq!(entries[i].1, found_entries[i].1);
            }
        }
    }

    /// Single-thread, single partition, multiple slabs.
    #[test]
    fn test_single_partition_multi_slab() {
        let num_slabs = 5 * partition::REPLAY_SLAB_COUNT;
        let entry_size = 1024;
        let parallelism = 1;
        do_multi_slab_test(num_slabs, entry_size, parallelism);
    }

    /// Single-thread, multiple partitions, multiple slabs.
    #[test]
    fn test_multi_partition_multi_slab() {
        let num_slabs = 10 * partition::REPLAY_SLAB_COUNT;
        let entry_size = 1024;
        let parallelism = 2;
        do_multi_slab_test(num_slabs, entry_size, parallelism);
        let parallelism = 4;
        log::info!("[TEST] Increasing parallelism to {parallelism}");
        do_multi_slab_test(num_slabs, entry_size, parallelism);
    }

    /// Multi-threads, multiple partitions, multiple slabs.
    #[test]
    fn test_multi_thread_multi_partition_multi_slab() {
        let parallelism = 2;
        let num_slabs = parallelism * 5 * partition::REPLAY_SLAB_COUNT;
        let entry_size = 1024;
        do_multi_threaded_multi_slab_test(num_slabs, entry_size, parallelism);
        let parallelism = 4;
        do_multi_threaded_multi_slab_test(num_slabs, entry_size, parallelism);
    }

    /// Multi-threads, multiple partitions, multiple slabs, failure injections.
    #[test]
    fn test_failure_injection() {
        let num_slabs = 1;
        let entry_size = 1024;
        let parallelism = 2;
        do_failure_injection_test(num_slabs, entry_size, parallelism);
        let num_slabs = 10 * partition::REPLAY_SLAB_COUNT;
        do_failure_injection_test(num_slabs, entry_size, parallelism);
        let parallelism = 4;
        do_failure_injection_test(num_slabs, entry_size, parallelism);
    }

    /// Multiple runs of test.
    #[cfg(not(debug_assertions))]
    #[test]
    fn stress_test_no_failure() {
        let parallelism = 8;
        let num_slabs = parallelism * 4 * partition::REPLAY_SLAB_COUNT;
        let entry_size = 1024;
        for _ in 0..10 {
            do_multi_threaded_multi_slab_test(num_slabs, entry_size, parallelism);
        }
    }

    /// Multiple runs of test.
    #[cfg(not(debug_assertions))]
    #[test]
    fn stress_test_failure_injection() {
        let parallelism = 8;
        let num_slabs = parallelism * 5 * partition::REPLAY_SLAB_COUNT;
        let entry_size = 1024;
        for _ in 0..10 {
            do_failure_injection_test(num_slabs, entry_size, parallelism);
        }
    }

    /// Write benchmark durations to csv file.
    fn write_durations(durations: &[(std::time::Duration, std::time::Duration)], filename: &str) {
        let mut writer = csv::Writer::from_path(filename).unwrap();
        for (since, d) in durations {
            writer
                .write_record(&[since.as_secs_f64().to_string(), d.as_secs_f64().to_string()])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn do_bench_test(
        storage_dir: &str,
        num_slabs: usize,
        entry_size: usize,
        parallelism: usize,
        write_delay: f64,
    ) {
        let name = "essai";
        reset_test(storage_dir, name);
        let num_entries = num_slabs * (SLAB_SIZE / entry_size);
        let mut entries = Vec::new();
        let mut durations = Vec::new();
        let global_start_time = std::time::Instant::now();
        {
            let manager = manager::Manager::new(storage_dir, name, parallelism).unwrap();
            let manager = Arc::new(manager);
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                let global_start_time = global_start_time.clone();
                threads.push(std::thread::spawn(move || {
                    let mut entries = Vec::new();
                    let mut persist_durations = Vec::new();
                    let start_idx = p * (num_entries / parallelism);
                    let end_idx = (p + 1) * (num_entries / parallelism);
                    let write_delay = if write_delay > 0.0 {
                        Some(write_delay)
                    } else {
                        None
                    };
                    for i in start_idx..end_idx {
                        let val = (i % 256) as u8;
                        let entry: Vec<u8> = vec![val; entry_size];
                        let lsn = manager.enqueue(entry.clone()).unwrap();
                        entries.push((entry, lsn));
                        let start_time = std::time::Instant::now();
                        manager
                            .controlled_persist(None, false, false, write_delay.clone())
                            .unwrap();
                        persist_durations.push((global_start_time.elapsed(), start_time.elapsed()));
                    }
                    (entries, persist_durations)
                }));
            }
            for t in threads {
                let (res, persist_durations) = t.join().unwrap();
                entries.extend(res);
                durations.extend(persist_durations);
            }
        }
        // Print to prevent compiler from optimizing out.
        log::info!("Entries: {}", entries.len());
        log::info!("Num Persists: {:?}", durations.len());
        // Write durations to csv file.
        let filename = "bench_durations1.csv";
        write_durations(&durations, filename);
        // Print avg, p50, p90 and p95.
        let mut total_duration = std::time::Duration::from_secs(0);
        let durations = durations.iter().map(|(_, d)| *d).collect::<Vec<_>>();
        for d in &durations {
            total_duration += *d;
        }
        let avg_duration = total_duration / durations.len() as u32;
        log::info!("Avg Persist Duration: {:?}", avg_duration);
        let mut durations = durations;
        durations.sort();
        let p50_duration = durations[durations.len() / 2];
        log::info!("P50 Persist Duration: {:?}", p50_duration);
        let p90_duration = durations[(durations.len() as f64 * 0.9) as usize];
        log::info!("P90 Persist Duration: {:?}", p90_duration);
        let p95_duration = durations[(durations.len() as f64 * 0.95) as usize];
        log::info!("P95 Persist Duration: {:?}", p95_duration);
        let p99_duration = durations[(durations.len() as f64 * 0.99) as usize];
        log::info!("P99 Persist Duration: {:?}", p99_duration);
        // Sort entries by lsn.
        entries.sort_by_key(|e| e.1);
        let hi_lsn = entries.last().unwrap().1;
        // Truncate, reinsert and time.
        let mut entries = Vec::new();
        let mut durations = Vec::new();
        {
            let manager = manager::Manager::new(STORAGE_DIR, name, parallelism).unwrap();
            manager.truncate(hi_lsn).unwrap();
            let mut threads = Vec::new();
            for p in 0..parallelism {
                let manager = manager.clone();
                let global_start_time = global_start_time.clone();
                threads.push(std::thread::spawn(move || {
                    let mut entries = Vec::new();
                    let mut persist_durations = Vec::new();
                    let start_idx = p * (num_entries / parallelism);
                    let end_idx = (p + 1) * (num_entries / parallelism);
                    let write_delay = if write_delay > 0.0 {
                        Some(write_delay)
                    } else {
                        None
                    };
                    for i in start_idx..end_idx {
                        let val = (i % 256) as u8;
                        let entry: Vec<u8> = vec![val; entry_size];
                        let lsn = manager.enqueue(entry.clone()).unwrap();
                        entries.push((entry, lsn));
                        let start_time = std::time::Instant::now();
                        manager
                            .controlled_persist(None, false, false, write_delay.clone())
                            .unwrap();
                        persist_durations.push((global_start_time.elapsed(), start_time.elapsed()));
                    }
                    (entries, persist_durations)
                }));
            }
            for t in threads {
                let (res, persist_durations) = t.join().unwrap();
                entries.extend(res);
                durations.extend(persist_durations);
            }
        }
        // Print to prevent compiler from optimizing out.
        log::info!("Entries: {}", entries.len());
        log::info!("Num Persists: {:?}", durations.len());
        // Write durations to csv file.
        let filename = "bench_durations2.csv";
        write_durations(&durations, filename);
        // Print avg, p50, p90 and p95.
        let durations = durations.iter().map(|(_, d)| *d).collect::<Vec<_>>();
        let mut total_duration = std::time::Duration::from_secs(0);
        for d in &durations {
            total_duration += *d;
        }
        let avg_duration = total_duration / durations.len() as u32;
        log::info!("Avg Persist Duration: {:?}", avg_duration);
        let mut durations = durations;
        durations.sort();
        let p50_duration = durations[durations.len() / 2];
        log::info!("P50 Persist Duration: {:?}", p50_duration);
        let p90_duration = durations[(durations.len() as f64 * 0.9) as usize];
        log::info!("P90 Persist Duration: {:?}", p90_duration);
        let p95_duration = durations[(durations.len() as f64 * 0.95) as usize];
        log::info!("P95 Persist Duration: {:?}", p95_duration);
        let p99_duration = durations[(durations.len() as f64 * 0.99) as usize];
        log::info!("P99 Persist Duration: {:?}", p99_duration);
    }

    #[test]
    fn bench_test() {
        // NOTE: Replace with EFS dir on AWS.
        let storage_dir = STORAGE_DIR;
        let parallelism = 1;
        let num_slabs = parallelism * 5;
        let entry_size = 1024;
        let write_delay = -0.005;
        do_bench_test(storage_dir, num_slabs, entry_size, parallelism, write_delay);
    }
}
