#[cfg(test)]
mod tests {
    use crate::bpm::journal;

    use self::page::BTreePage;
    use rand::seq::SliceRandom;
    use std::sync::Arc;

    use super::super::*;
    use manager::ElemType;

    const TEST_STORAGE_DIR: &str = "./storage"; // /mnt/efs/fs1/low_level_systems_storage
    const TEST_NAME: &str = "essai";
    const PAGE_SIZE: usize = 8192;

    fn make_kv(i: usize, key_size: usize, val_size: usize) -> (Vec<u8>, Vec<u8>) {
        let key = i.to_be_bytes().to_vec();
        let val = i.to_be_bytes().to_vec();
        let key = key.iter().cycle().take(key_size).cloned().collect();
        let val = val.iter().cycle().take(val_size).cloned().collect();
        (key, val)
    }

    /// Reset test.
    fn reset_test() {
        std::env::set_var("RUST_LOG", "warn");
        env_logger::builder()
            .format_timestamp(None)
            .format_level(true)
            .format_module_path(false)
            .format_target(false)
            .try_init()
            .unwrap_or(());
        reset_btree(TEST_STORAGE_DIR, TEST_NAME);
    }

    fn test_basic_ops(early_return: bool, with_completion: bool, keep_pages: bool) {
        reset_test();
        let setup = manager::Setup {
            enable_early_return: early_return,
            enable_completion: with_completion,
            enable_keep_pages: keep_pages,
        };
        {
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree
                .insert("hello".as_bytes(), "world".as_bytes())
                .unwrap();
            let value = btree.get("hello".as_bytes()).unwrap().unwrap();
            assert_eq!(value, "world".as_bytes());
            btree.show_structure(ElemType::Str, ElemType::Str);
            if !keep_pages {
                std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
                btree.bpm.check_all_unpinned().unwrap();
            }
        }
        {
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            let value = btree.get("hello".as_bytes()).unwrap().unwrap();
            assert_eq!(value, "world".as_bytes());
            btree.delete("hello".as_bytes()).unwrap();
            let value = btree.get("hello".as_bytes()).unwrap();
            assert_eq!(value, None);
            btree.show_structure(ElemType::Str, ElemType::Str);
            btree
                .insert("hello1".as_bytes(), "world1".as_bytes())
                .unwrap();
            if !keep_pages {
                std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
                btree.bpm.check_all_unpinned().unwrap();
            }
        }
        {
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            let value = btree.get("hello".as_bytes()).unwrap();
            assert_eq!(value, None);
            let value = btree.get("hello1".as_bytes()).unwrap().unwrap();
            assert_eq!(value, "world1".as_bytes());
            btree.show_structure(ElemType::Str, ElemType::Str);
            if !keep_pages {
                std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
                btree.bpm.check_all_unpinned().unwrap();
            }
        }
    }

    #[test]
    fn basic_test() {
        test_basic_ops(true, true, true);
        test_basic_ops(true, true, false);
        test_basic_ops(false, true, true);
        test_basic_ops(false, true, false);
    }

    #[test]
    fn basic_test_with_recovery() {
        test_basic_ops(true, false, false);
    }

    fn test_root_split(early_return: bool, with_completion: bool, keep_pages: bool) {
        reset_test();
        let setup = manager::Setup {
            enable_early_return: early_return,
            enable_completion: with_completion,
            enable_keep_pages: keep_pages,
        };
        let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
        btree.change_setup(setup.clone());
        let (key_size, val_size) = (8, 256);
        let (dummy_k, dummy_v) = make_kv(0, key_size, val_size);
        let kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let num_keys = (3 * PAGE_SIZE) / (2 * kv_size);
        for i in 0..num_keys {
            let (key, val) = make_kv(i, key_size, val_size);
            btree.insert(&key, &val).unwrap();
        }
        if !keep_pages {
            println!("Waiting due to early return...");
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::mem::drop(btree);
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree
        };
        for i in 0..num_keys {
            let (key, val) = make_kv(i, key_size, val_size);
            let value = btree.get(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Rev because delete only merges when left sibling empty.
        for i in (0..num_keys).rev() {
            let (key, _val) = make_kv(i, key_size, val_size);
            btree.delete(&key).unwrap();
        }
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::mem::drop(btree);
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree
        };
        for i in 0..num_keys {
            let (key, _val) = make_kv(i, key_size, val_size);
            let value = btree.get(&key).unwrap();
            assert_eq!(value, None);
        }
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
    }

    #[test]
    fn simple_root_split_test() {
        test_root_split(true, true, false);
        test_root_split(true, true, true);
        test_root_split(false, true, false);
        test_root_split(false, true, true);
    }

    fn test_single_threaded_multi_level(
        early_return: bool,
        with_completion: bool,
        keep_pages: bool,
    ) {
        reset_test();
        let setup = manager::Setup {
            enable_early_return: early_return,
            enable_completion: with_completion,
            enable_keep_pages: keep_pages,
        };
        let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
        btree.change_setup(setup.clone());
        let (inner_key_size, inner_val_size) = (8, 8);
        let (dummy_k, dummy_v) = make_kv(0, inner_key_size, inner_val_size);
        let inner_kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let (key_size, val_size) = (8, 1024 - 64);
        let (dummy_k, dummy_v) = make_kv(0, key_size, val_size);
        let kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let num_inner_entries = (PAGE_SIZE + inner_kv_size - 1) / (inner_kv_size);
        let num_leaf_entries = (num_inner_entries * PAGE_SIZE + kv_size - 1) / (kv_size);
        // println!("Num entries: {}", num_leaf_entries);
        for i in 0..num_leaf_entries {
            let (key, val) = make_kv(i, key_size, val_size);
            btree.insert(&key, &val).unwrap();
            // println!("Inserted key: {}", i);
        }
        assert!(btree.root_height() == 2);
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::mem::drop(btree);
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
            btree
        };
        for i in 0..num_leaf_entries {
            let (key, val) = make_kv(i, key_size, val_size);
            println!("Key {i}.");
            let value = btree.get(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }
        // btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        for i in (0..num_leaf_entries).rev() {
            if i % 2 == 0 {
                let (key, _val) = make_kv(i, key_size, val_size);
                btree.delete(&key).unwrap();
                // println!("Deleted key: {}", i);
            }
        }
        assert!(btree.root_height() == 1);
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::mem::drop(btree);
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree
        };
        for i in 0..num_leaf_entries {
            let (key, val) = make_kv(i, key_size, val_size);
            let value = btree.get(&key).unwrap();
            if i % 2 == 0 {
                assert_eq!(value, None);
            } else {
                assert_eq!(value.unwrap(), val);
            }
        }
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Delete the rest
        for i in (0..num_leaf_entries).rev() {
            if i % 2 != 0 {
                let (key, _val) = make_kv(i, key_size, val_size);
                btree.delete(&key).unwrap();
            }
        }
        assert!(btree.root_height() == 0);
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::mem::drop(btree);
            let btree = Manager::new(1, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            btree
        };
        for i in 0..num_leaf_entries {
            let (key, _val) = make_kv(i, key_size, val_size);
            let value = btree.get(&key).unwrap();
            assert_eq!(value, None);
        }
        let (active_pages, _free_pages) = btree.bpm.page_statistics().unwrap();
        assert_eq!(active_pages, 1);
    }

    #[test]
    fn simple_multi_level_split_test() {
        test_single_threaded_multi_level(true, true, false);
        test_single_threaded_multi_level(true, true, true);
        test_single_threaded_multi_level(false, true, false);
        test_single_threaded_multi_level(false, true, true);
    }

    #[test]
    fn simple_multi_level_split_test_with_recovery() {
        test_single_threaded_multi_level(true, false, false);
    }

    fn test_multi_threaded_ops(early_return: bool, with_completion: bool, keep_pages: bool) {
        reset_test();
        let setup = manager::Setup {
            enable_early_return: early_return,
            enable_completion: with_completion,
            enable_keep_pages: keep_pages,
        };
        let num_threads = 4;
        let btree =
            Arc::new(Manager::new(num_threads, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap());
        btree.change_setup(setup.clone());
        let (inner_key_size, inner_val_size) = (8, 8);
        let (dummy_k, dummy_v) = make_kv(0, inner_key_size, inner_val_size);
        let inner_kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let (key_size, val_size) = (8, 1024 - 64);
        let (dummy_k, dummy_v) = make_kv(0, key_size, val_size);
        let kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let num_inner_entries = (PAGE_SIZE + inner_kv_size - 1) / (inner_kv_size);
        let num_leaf_entries = (num_inner_entries * PAGE_SIZE + kv_size - 1) / (kv_size);
        // println!("Num entries: {}", num_leaf_entries);
        let mut indexes = (0..num_leaf_entries).collect::<Vec<usize>>();
        indexes.shuffle(&mut rand::thread_rng());
        // Split
        let mut threads = vec![];
        let chunk_size = num_leaf_entries / num_threads;
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    let (key, val) = make_kv(i, key_size, val_size);
                    btree.insert(&key, &val).unwrap();
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::thread::sleep(std::time::Duration::from_secs_f64(1.0));
            std::mem::drop(btree);
            let btree = Manager::new(num_threads, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            Arc::new(btree)
        };
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        let start_time = std::time::Instant::now();
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    let (key, val) = make_kv(i, key_size, val_size);
                    let value = btree.get(&key).unwrap().unwrap();
                    assert_eq!(value, val);
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        println!(
            "Elapsed time: {:?}. Num reads: {}. Num Threads: {num_threads}",
            start_time.elapsed(),
            num_leaf_entries
        );
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Delete half
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        let start_time = std::time::Instant::now();
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    if i % 2 == 0 {
                        let (key, _val) = make_kv(i, key_size, val_size);
                        btree.delete(&key).unwrap();
                    }
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        println!(
            "Elapsed time: {:?}. Num deletes: {}. Num Threads: {num_threads}",
            start_time.elapsed(),
            num_leaf_entries / 2
        );
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Check content
        let btree = if with_completion {
            btree
        } else {
            std::mem::drop(btree);
            let btree = Manager::new(num_threads, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            Arc::new(btree)
        };
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    let (key, val) = make_kv(i, key_size, val_size);
                    let value = btree.get(&key).unwrap();
                    if i % 2 == 0 {
                        assert_eq!(value, None);
                    } else {
                        if value.is_none() {
                            println!("Key not found: {i}");
                            btree.show_structure(ElemType::USIZE, ElemType::Bytes(Some(8)));
                        }
                        assert_eq!(value.unwrap(), val);
                    }
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        btree.bpm.check_all_unpinned().unwrap();
        // Delete the rest
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    if i % 2 != 0 {
                        let (key, _val) = make_kv(i, key_size, val_size);
                        btree.delete(&key).unwrap();
                    }
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        // Check content
        let btree = if with_completion {
            btree
        } else {
            // Force recovery
            std::thread::sleep(std::time::Duration::from_secs_f64(1.0));
            std::mem::drop(btree);
            let btree = Manager::new(num_threads, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
            btree.change_setup(setup.clone());
            Arc::new(btree)
        };
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    let (key, _val) = make_kv(i, key_size, val_size);
                    let value = btree.get(&key).unwrap();
                    assert_eq!(value, None);
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        if !keep_pages {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            btree.bpm.check_all_unpinned().unwrap();
        }
        let (active_pages, free_pages) = btree.bpm.page_statistics().unwrap();
        println!("Active pages: {}, Free pages: {}", active_pages, free_pages);
        // Only root should be active.
        assert_eq!(active_pages, 1);
    }

    #[test]
    fn multi_thread_ops() {
        test_multi_threaded_ops(true, true, false);
        test_multi_threaded_ops(true, true, true);
        test_multi_threaded_ops(false, true, false);
        test_multi_threaded_ops(false, true, true);
    }

    #[test]
    fn multi_thread_ops_with_recovery() {
        test_multi_threaded_ops(true, false, false);
    }

    #[test]
    fn dummy_test_page_read() {
        let journal = journal::Journal::new(4, PAGE_SIZE, TEST_STORAGE_DIR, TEST_NAME).unwrap();
        let page = journal.read_page(261).unwrap();
        println!("PageId={}, Version={}", page.page_id(), page.page_version());
    }

    #[test]
    fn test_bench() {
        reset_test();
        let setup = manager::Setup {
            enable_early_return: true,
            enable_completion: true,
            enable_keep_pages: true,
        };
        let num_threads = 4;
        let page_size = 8192;
        let btree =
            Arc::new(Manager::new(num_threads, page_size, TEST_STORAGE_DIR, TEST_NAME).unwrap());
        // btree.wal.set_replicas(Some(("".into(), "".into()))).unwrap();
        btree.change_setup(setup.clone());
        let (inner_key_size, inner_val_size) = (8, 8);
        let (dummy_k, dummy_v) = make_kv(0, inner_key_size, inner_val_size);
        let inner_kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let (key_size, val_size) = (8, 1024 - 64);
        let (dummy_k, dummy_v) = make_kv(0, key_size, val_size);
        let kv_size = BTreePage::total_len(&dummy_k, &dummy_v);
        let num_inner_entries = (PAGE_SIZE + inner_kv_size - 1) / (inner_kv_size);
        let num_leaf_entries = (num_inner_entries * PAGE_SIZE + kv_size - 1) / (kv_size);
        let num_leaf_entries = num_leaf_entries;
        println!("Num entries: {}", num_leaf_entries);
        let mut indexes = (0..num_leaf_entries).collect::<Vec<usize>>();
        indexes.shuffle(&mut rand::thread_rng());
        // Initial Inserts
        let mut threads = vec![];
        let chunk_size = num_leaf_entries / num_threads;
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for (idx, i) in chunk.into_iter().enumerate() {
                    let (key, val) = make_kv(i, key_size, val_size);
                    btree.insert(&key, &val).unwrap();
                    if idx % 1000 == 0 {
                        println!("Inserted key: {}", idx);
                    }
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        println!("Done Inserting.");
        // Reads
        btree.reset_stats();
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        let start_time = std::time::Instant::now();
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                for i in chunk {
                    let (key, val) = make_kv(i, key_size, val_size);
                    let value = btree.get(&key).unwrap().unwrap();
                    assert_eq!(value, val);
                }
            });
            threads.push(handle);
        });
        threads
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        println!(
            "Elapsed time: {:?}. Num reads: {}. Num Threads: {num_threads}",
            start_time.elapsed(),
            num_leaf_entries
        );
        // Updates
        btree.show_stats();
        btree.reset_stats();
        indexes.shuffle(&mut rand::thread_rng());
        let mut threads = vec![];
        let start_time = std::time::Instant::now();
        indexes.chunks(chunk_size).for_each(|chunk| {
            let btree = btree.clone();
            let chunk = chunk.to_vec();
            let handle = std::thread::spawn(move || {
                let mut durations = vec![];
                for i in chunk {
                    let (key, val) = make_kv(i, key_size, val_size);
                    let val = val.iter().map(|x| (x / 2) + 1).collect::<Vec<_>>();
                    let start_time = std::time::Instant::now();
                    btree.insert(&key, &val).unwrap();
                    durations.push(start_time.elapsed());
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                durations
            });
            threads.push(handle);
        });
        let mut durations = vec![];
        threads.into_iter().for_each(|handle| {
            let durs = handle.join().unwrap();
            durations.extend(durs);
        });
        println!(
            "Elapsed time: {:?}. Num updates: {}. Num Threads: {num_threads}",
            start_time.elapsed(),
            num_leaf_entries
        );
        // P50 duration
        durations.sort();
        let p50 = durations[durations.len() / 2];
        // P10 duration
        let p10 = durations[durations.len() / 10];
        // P90 duration
        let p90 = durations[(durations.len() * 9) / 10];
        // Avg
        let avg = durations.iter().sum::<std::time::Duration>() / durations.len() as u32;
        println!(
            "Insert stats: P50={:?}, P10={:?}, P90={:?}, Avg={:?}",
            p50, p10, p90, avg
        );
        btree.show_stats();
    }
}
