#[cfg(test)]
mod tests {
    use crate::bpm::*;
    use std::sync::Arc;
    const STORAGE_DIR: &str = "storage";

    /// Reset test.
    fn reset_test(storage_dir: &str, name: &str) {
        std::env::set_var("RUST_LOG", "debug");
        env_logger::builder()
            .format_timestamp(None)
            .format_level(true)
            .format_module_path(false)
            .format_target(false)
            .try_init()
            .unwrap_or(());
        reset_bpm(storage_dir, name);
    }

    #[test]
    fn basic_test() {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let parallelism = 1;
        let page_size = 1024;
        let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
        let page = bpm.create_page().unwrap();
        let page_id = {
            let mut page = page.write().unwrap();
            let data_offset = Page::data_offset();
            page.write(data_offset, b"Hello world!");
            page.update_version(1);
            bpm.write_back(&mut page).unwrap();
            page.page_id()
        };
        bpm.unpin(page_id).unwrap();
        let page = bpm.pin(page_id).unwrap();
        {
            let page = page.read().unwrap();
            let data_offset = Page::data_offset();
            let data = page.read(data_offset, 12);
            assert_eq!(page.page_version(), 1);
            assert_eq!(data, b"Hello world!");
        }
        bpm.unpin(page_id).unwrap();
    }

    fn do_multi_page_test(
        parallelism: usize,
        page_size: usize,
        name: &str,
        num_pages: usize,
        inject_failure: bool,
        stress_test_count: usize,
    ) {
        assert!(num_pages % parallelism == 0);
        assert!(num_pages > 0);
        assert!(page_size > 128);
        let short_sleep_secs = 0.25;
        let long_sleep_secs = 0.5;
        let data_offset = Page::data_offset();
        let data_len = page_size - Page::data_offset();
        // Write pages.
        let page_ids = {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let num_pages = num_pages / parallelism;
                let thread = std::thread::spawn(move || {
                    let start_idx = p * num_pages;
                    let end_idx = start_idx + num_pages;
                    let mut page_ids = vec![];
                    for i in start_idx..end_idx {
                        let page_content = vec![(i % 256) as u8; data_len];
                        let page = bpm.create_page().unwrap();
                        let page_id = {
                            let mut page = page.write().unwrap();
                            page.write(data_offset, &page_content);
                            page.update_version(1);
                            bpm.write_back(&mut page).unwrap();
                            page.page_id()
                        };
                        bpm.unpin(page_id).unwrap();
                        page_ids.push((page_id, i));
                    }
                    page_ids
                });
                threads.push(thread);
            }
            let mut page_ids = vec![];
            for thread in threads {
                let mut thread_page_ids = thread.join().unwrap();
                page_ids.append(&mut thread_page_ids);
            }
            assert_eq!(page_ids.len(), num_pages);
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
            page_ids
        };
        // Check pages.
        std::thread::sleep(std::time::Duration::from_secs_f64(long_sleep_secs));
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let num_pages = num_pages / parallelism;
                let page_ids = page_ids.clone();
                let thread = std::thread::spawn(move || {
                    let start_idx = p * num_pages;
                    let end_idx = start_idx + num_pages;
                    for i in start_idx..end_idx {
                        let (page_id, content) = page_ids[i];
                        let expected_content = vec![(content % 256) as u8; data_len];
                        let expected_version = 1;
                        let page = bpm.pin(page_id).unwrap();
                        {
                            let page = page.read().unwrap();
                            let actual_content = page.read(data_offset, data_len);
                            assert_eq!(actual_content, expected_content);
                            assert_eq!(page.page_version(), expected_version);
                        }
                        bpm.unpin(page_id).unwrap();
                    }
                });
                threads.push(thread);
            }
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
        if !inject_failure {
            return;
        }
        // Do writes that are staged, then fail right after.
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let thread = std::thread::spawn(move || {
                    let (page_id, content) = page_ids[p];
                    let new_content = content + 1;
                    let new_content = vec![(new_content % 256) as u8; data_len];
                    let page = bpm.pin(page_id).unwrap();
                    {
                        let mut page = page.write().unwrap();
                        page.write(data_offset, &new_content);
                        page.update_version(2);
                        bpm.controlled_write_back(&mut page, false, true, Some(short_sleep_secs))
                            .unwrap();
                    }
                    bpm.unpin(page_id).unwrap();
                });
                threads.push(thread);
            }
            for thread in threads {
                let _ = thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        };
        // Check that write completes during recovery despite the injected failure.
        std::thread::sleep(std::time::Duration::from_secs_f64(long_sleep_secs));
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let thread = std::thread::spawn(move || {
                    let (page_id, content) = page_ids[p];
                    let new_content = content + 1;
                    let expected_content = vec![(new_content % 256) as u8; data_len];
                    let expected_version = 2;
                    let page = bpm.pin(page_id).unwrap();
                    {
                        let page = page.read().unwrap();
                        let actual_content = page.read(data_offset, data_len);
                        assert_eq!(page.page_version(), expected_version);
                        assert_eq!(actual_content, expected_content);
                    }
                    bpm.unpin(page_id).unwrap();
                });
                threads.push(thread);
            }
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
        // Do writes that fail before staging.
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let thread = std::thread::spawn(move || {
                    let (page_id, content) = page_ids[p];
                    let new_content = content + 100;
                    let new_content = vec![(new_content % 256) as u8; data_len];
                    let page = bpm.pin(page_id).unwrap();
                    {
                        let mut page = page.write().unwrap();
                        page.write(data_offset, &new_content);
                        page.update_version(3);
                        let err = bpm.controlled_write_back(
                            &mut page,
                            true,
                            false,
                            Some(short_sleep_secs),
                        );
                        assert!(err.is_err());
                    }
                    bpm.unpin(page_id).unwrap();
                });
                threads.push(thread);
            }
            for thread in threads {
                let _ = thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
        // Check that writes that complete before staging are not recovered.
        std::thread::sleep(std::time::Duration::from_secs_f64(long_sleep_secs));
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let thread = std::thread::spawn(move || {
                    let (page_id, content) = page_ids[p];
                    let new_content = content + 1;
                    let expected_content = vec![(new_content % 256) as u8; data_len];
                    let expected_version = 2;
                    let page = bpm.pin(page_id).unwrap();
                    {
                        let page = page.read().unwrap();
                        let actual_content = page.read(data_offset, data_len);
                        assert_eq!(actual_content, expected_content);
                        assert_eq!(page.page_version(), expected_version);
                    }
                    bpm.unpin(page_id).unwrap();
                });
                threads.push(thread);
            }
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
        // Check all pages one last time.
        if stress_test_count == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs_f64(long_sleep_secs));
        {
            let bpm = manager::Manager::new(parallelism, page_size, STORAGE_DIR, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for _ in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let stress_test_count = stress_test_count / parallelism;
                let thread = std::thread::spawn(move || {
                    for _ in 0..stress_test_count {
                        let i: usize = rand::random::<usize>() % page_ids.len();
                        let (page_id, content) = page_ids[i];
                        let (expected_content, expected_version) = if i >= parallelism {
                            let expected_content = vec![(content % 256) as u8; data_len];
                            let expected_version = 1;
                            (expected_content, expected_version)
                        } else {
                            let expected_content = vec![((content + 1) % 256) as u8; data_len];
                            let expected_version = 2;
                            (expected_content, expected_version)
                        };
                        let page = bpm.pin(page_id).unwrap();
                        {
                            let page = page.read().unwrap();
                            let actual_content = page.read(data_offset, data_len);
                            assert_eq!(page.page_version(), expected_version);
                            assert_eq!(actual_content, expected_content);
                        }
                        bpm.unpin(page_id).unwrap();
                    }
                });
                threads.push(thread);
            }
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
    }

    /// Write benchmark durations to csv file.
    fn write_durations_to_csv(
        mode: &str,
        durations: &[(std::time::Duration, std::time::Duration)],
        filename: &str,
    ) {
        let mut writer = csv::Writer::from_path(filename).unwrap();
        for (since, d) in durations {
            writer
                .write_record(&[
                    mode.to_string(),
                    since.as_secs_f64().to_string(),
                    d.as_secs_f64().to_string(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn do_bench_test(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
        num_pages: usize,
        stress_test_count: usize,
    ) {
        assert!(num_pages % parallelism == 0);
        assert!(num_pages > 0);
        assert!(page_size > 128);
        let long_sleep_secs = 0.5;
        let data_offset = Page::data_offset();
        let data_len = page_size - Page::data_offset();
        // Create pages.
        let global_start_time = std::time::Instant::now();
        let mut create_durations = vec![];
        let page_ids = {
            let bpm = manager::Manager::new(parallelism, page_size, storage_dir, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for p in 0..parallelism {
                let bpm = bpm.clone();
                let num_pages = num_pages / parallelism;
                let mut create_durations = vec![];
                let global_start_time = global_start_time.clone();
                let thread = std::thread::spawn(move || {
                    let start_idx = p * num_pages;
                    let end_idx = start_idx + num_pages;
                    let mut page_ids = vec![];
                    for i in start_idx..end_idx {
                        let page_content = vec![(i % 256) as u8; data_len];
                        let start_time = std::time::Instant::now();
                        let page = bpm.create_page().unwrap();
                        create_durations.push((global_start_time.elapsed(), start_time.elapsed()));
                        let page_id = {
                            let mut page = page.write().unwrap();
                            page.write(data_offset, &page_content);
                            page.update_version(1);
                            bpm.write_back(&mut page).unwrap();
                            page.page_id()
                        };
                        bpm.unpin(page_id).unwrap();
                        page_ids.push((page_id, i));
                    }
                    (page_ids, create_durations)
                });
                threads.push(thread);
            }
            let mut page_ids = vec![];
            for thread in threads {
                let (mut thread_page_ids, mut ds) = thread.join().unwrap();
                page_ids.append(&mut thread_page_ids);
                create_durations.append(&mut ds);
            }
            assert_eq!(page_ids.len(), num_pages);
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
            page_ids
        };
        // Do a bunch of reads and writes.
        std::thread::sleep(std::time::Duration::from_secs_f64(long_sleep_secs));
        // Just to prevent optimization.
        let mut checksums = 0;
        let mut write_durations = vec![];
        let mut read_durations = vec![];
        let mut read_pin_durations = vec![];
        let mut write_pin_durations = vec![];
        let mut write_back_durations = vec![];
        {
            let bpm = manager::Manager::new(parallelism, page_size, storage_dir, name).unwrap();
            let bpm = Arc::new(bpm);
            let mut threads = vec![];
            for _ in 0..parallelism {
                let bpm = bpm.clone();
                let page_ids = page_ids.clone();
                let stress_test_count = stress_test_count / parallelism;
                let mut checksums = 0;
                let global_start_time = global_start_time.clone();
                let thread = std::thread::spawn(move || {
                    let mut write_durations = vec![];
                    let mut read_durations = vec![];
                    let mut read_pin_durations = vec![];
                    let mut write_pin_durations = vec![];
                    let mut write_back_durations = vec![];
                    for _ in 0..stress_test_count {
                        // Do a read
                        let i: usize = rand::random::<usize>() % page_ids.len();
                        let (page_id, _) = page_ids[i];
                        let start_time = std::time::Instant::now();
                        let page = bpm.pin(page_id).unwrap();
                        read_pin_durations
                            .push((global_start_time.elapsed(), start_time.elapsed()));
                        {
                            let page = page.read().unwrap();
                            checksums += page.checksum();
                        }
                        bpm.unpin(page_id).unwrap();
                        read_durations.push((global_start_time.elapsed(), start_time.elapsed()));
                        // Do a write.
                        if i % 10 != 0 {
                            continue;
                        }
                        let i: usize = rand::random::<usize>() % page_ids.len();
                        let content: u8 = rand::random::<u8>();
                        let content = vec![content; data_len];
                        let (page_id, _) = page_ids[i];
                        let start_time = std::time::Instant::now();
                        let page = bpm.pin(page_id).unwrap();
                        write_pin_durations
                            .push((global_start_time.elapsed(), start_time.elapsed()));
                        {
                            let mut page = page.write().unwrap();
                            page.write(data_offset, &content);
                            let version = page.page_version() + 1;
                            page.update_version(version);
                            let start_time = std::time::Instant::now();
                            bpm.write_back(&mut page).unwrap();
                            write_back_durations
                                .push((global_start_time.elapsed(), start_time.elapsed()));
                        }
                        bpm.unpin(page_id).unwrap();
                        write_durations.push((global_start_time.elapsed(), start_time.elapsed()));
                    }
                    (
                        checksums,
                        write_durations,
                        read_durations,
                        write_pin_durations,
                        write_back_durations,
                        read_pin_durations,
                    )
                });
                threads.push(thread);
            }
            for thread in threads {
                let (c, mut wds, mut rds, mut wpds, mut wbds, mut rpds) = thread.join().unwrap();
                checksums += c;
                write_durations.append(&mut wds);
                read_durations.append(&mut rds);
                write_pin_durations.append(&mut wpds);
                write_back_durations.append(&mut wbds);
                read_pin_durations.append(&mut rpds);
            }
            // To prevent optimization.
            println!("Checksums: {}", checksums);
            assert!(matches!(bpm.check_all_unpinned(), Ok(())));
        }
        // Print P50, P90 durations.
        create_durations.sort_by_key(|(_, d)| d.as_nanos());
        write_durations.sort_by_key(|(_, d)| d.as_nanos());
        read_durations.sort_by_key(|(_, d)| d.as_nanos());
        let p50 = |durations: &Vec<(std::time::Duration, std::time::Duration)>| {
            let idx = durations.len() / 2;
            durations[idx].1.clone()
        };
        let p90 = |durations: &Vec<(std::time::Duration, std::time::Duration)>| {
            let idx = (durations.len() as f64 * 0.9) as usize;
            durations[idx].1.clone()
        };
        let avg = |durations: &Vec<(std::time::Duration, std::time::Duration)>| {
            let sum: std::time::Duration = durations.iter().map(|(_, d)| *d).sum();
            let avg = sum / durations.len() as u32;
            avg
        };
        println!(
            "Create durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&create_durations),
            p50(&create_durations),
            p90(&create_durations)
        );
        println!(
            "Write durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&write_durations),
            p50(&write_durations),
            p90(&write_durations)
        );
        println!(
            "Read durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&read_durations),
            p50(&read_durations),
            p90(&read_durations)
        );
        println!(
            "Write pin durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&write_pin_durations),
            p50(&write_pin_durations),
            p90(&write_pin_durations)
        );
        println!(
            "Write back durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&write_back_durations),
            p50(&write_back_durations),
            p90(&write_back_durations)
        );
        println!(
            "Read pin durations: avg={:?}, p50={:?}, p90={:?}",
            avg(&read_pin_durations),
            p50(&read_pin_durations),
            p90(&read_pin_durations)
        );
        write_durations_to_csv("Create", &create_durations, "bpm_create_bench.csv");
        write_durations_to_csv("Write", &write_durations, "bpm_write_bench.csv");
        write_durations_to_csv("Read", &read_durations, "bpm_read_bench.csv");
    }

    #[test]
    fn test_multiple_pages_single_thread() {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let parallelism = 1;
        let page_size = 1024;
        let num_pages = 10 * parallelism;
        do_multi_page_test(parallelism, page_size, name, num_pages, true, 0);
    }

    #[test]
    fn test_multiple_pages_multi_thread() {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let parallelism = 8;
        let page_size = 1024;
        let num_pages = 10 * parallelism;
        do_multi_page_test(parallelism, page_size, name, num_pages, true, 0);
    }

    #[test]
    fn test_stress_test() {
        let name = "essai";
        reset_test(STORAGE_DIR, name);
        let parallelism = 8;
        let page_size = 1024;
        let num_pages = 1000 * parallelism;
        let stress_test_count = 10000 * parallelism;
        do_multi_page_test(
            parallelism,
            page_size,
            name,
            num_pages,
            true,
            stress_test_count,
        );
    }

    #[test]
    fn test_bench() {
        let name = "essai";
        // NOTE: Replace with EFS dir on AWS.
        let storage_dir = STORAGE_DIR;
        reset_test(storage_dir, name);
        let parallelism = 4;
        let page_size = 1024;
        let mul = 128;
        let num_pages = mul * parallelism;
        let stress_test_count = mul * parallelism;
        do_bench_test(
            parallelism,
            page_size,
            storage_dir,
            name,
            num_pages,
            stress_test_count,
        );
    }
}
