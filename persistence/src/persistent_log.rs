use super::SUBSYSTEM_NAME;
use common::scaling_state::ScalingStateManager;
use common::{InstanceInfo, MetricsManager};
use low_level_systems::btree::wal_provider::WalProvider;
use low_level_systems::LLError;

use low_level_systems::wal;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{atomic, Arc};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

const WAL_PARALLELISM: usize = 8;
const USE_AZ_PLUS_1: bool = false;

/// A lockful implementation of persistent log.
/// Probably should not have more than two concurrent flushers.
#[derive(Clone)]
pub struct PersistentLog {
    pub wal: Arc<wal::Manager>,
    pub incarnation: i64,
    pub caller_mems: Arc<RwLock<Vec<i32>>>,
    pub default_caller_mem: Arc<atomic::AtomicI32>,
    metrics_manager: Arc<MetricsManager>,
    scaling_manager: Arc<ScalingStateManager>,
    handle: Handle,
}

impl PersistentLog {
    /// Create a new persistent log.
    pub async fn new(instance_info: Arc<InstanceInfo>, incarnation: i64) -> Result<Self, String> {
        let scaling_manager = ScalingStateManager::new(
            SUBSYSTEM_NAME,
            &instance_info.namespace,
            &instance_info.identifier,
        )
        .await;
        let metrics_manager = MetricsManager::new(
            SUBSYSTEM_NAME,
            &instance_info.namespace,
            &instance_info.identifier,
        )
        .await;
        scaling_manager.start_refresh_thread().await;
        scaling_manager.start_rescaling_thread().await;
        metrics_manager.start_metrics_pushing_thread().await;
        let handle = Handle::current();
        let plog = tokio::task::block_in_place(move || {
            Self::new_sync(
                handle,
                instance_info,
                metrics_manager,
                scaling_manager,
                incarnation,
            )
        })?;
        Ok(plog)
    }

    /// Sync version of new.
    fn new_sync(
        handle: Handle,
        instance_info: Arc<InstanceInfo>,
        metrics_manager: MetricsManager,
        scaling_manager: ScalingStateManager,
        incarnation: i64,
    ) -> Result<Self, String> {
        println!("Creating persistent log!");
        let name = instance_info.identifier.clone();
        let storage_dir = common::storage_path(&instance_info);
        let _ = std::fs::create_dir_all(&storage_dir);
        let wal = Arc::new(
            wal::Manager::new_with_incarnation(&storage_dir, &name, WAL_PARALLELISM, incarnation)
                .unwrap(),
        );
        let plog = PersistentLog {
            handle,
            metrics_manager: Arc::new(metrics_manager),
            scaling_manager: Arc::new(scaling_manager),
            wal,
            incarnation,
            caller_mems: Arc::new(RwLock::new(Vec::new())),
            default_caller_mem: Arc::new(atomic::AtomicI32::new(512)),
        };
        println!("Created persistent log!");
        Ok(plog)
    }

    pub fn set_default_caller_mem(&self, mem: i32) {
        self.default_caller_mem
            .store(mem, atomic::Ordering::Relaxed);
    }

    pub async fn enqueue(&self, entry: Vec<u8>, mem: Option<i32>) -> i64 {
        let lsn = self.wal.enqueue(entry).unwrap();
        let mem = mem.unwrap_or_else(|| self.default_caller_mem.load(atomic::Ordering::Relaxed));
        self.caller_mems.write().await.push(mem);
        lsn
    }

    pub async fn flush(&self) {
        self.flush_at(None).await;
    }

    pub async fn flush_at(&self, lsn: Option<i64>) {
        let wal = self.wal.clone();
        tokio::task::spawn_blocking(move || {
            wal.persist(lsn).unwrap();
        })
        .await
        .unwrap();
        let metrics = {
            let mut caller_mems = self.caller_mems.write().await;
            caller_mems.drain(..).collect::<Vec<_>>()
        };
        if !metrics.is_empty() {
            let metrics = super::rescaler::WalMetric { user_mems: metrics };
            let metrics = bincode::serialize(&metrics).unwrap();
            self.metrics_manager.accumulate_metric(metrics).await;
        }
    }

    /// Check instances to replicate to.
    pub async fn check_replicas(&self, terminating: bool) {
        if terminating {
            tokio::task::block_in_place(|| {
                self.wal.set_replicas(None).unwrap();
            });
        }
        // Read instances.
        let mut new_instances = Vec::new();
        let mut scaling_state = (*self.scaling_manager.current_scaling_state().await).clone();
        self.scaling_manager
            .cleanup_instances(&mut scaling_state)
            .await;
        let all_instances = scaling_state.subsys_state.peers.get("replica").unwrap();

        let mut all_instances = all_instances.values().cloned().collect::<Vec<_>>();
        // println!("Changing instances. Found: {all_instances:?}.");
        // Order join_time. This to maximize reuse.
        all_instances.sort_by(|x1, x2| {
            match x1.join_time.cmp(&x2.join_time) {
                Ordering::Equal => {
                    // Just to handle unlikely same join times.
                    x1.instance_info.peer_id.cmp(&x2.instance_info.peer_id)
                }
                o => o,
            }
        });
        let mut az_counts: HashMap<String, u64> = all_instances
            .iter()
            .map(|instance| (instance.instance_info.az.clone().unwrap(), 0))
            .collect();
        // We want to tolerate AZ + 1 failures, so we need at least three AZs.
        // We need 6 nodes across 3 AZs, or 5 nodes across 5 AZs.
        // TODO(Amadou): Fargate is not using us-east-2a anymore. Figure this out.
        let mut num_azs = 0;
        let mut is_safe = false;
        for instance in all_instances {
            if USE_AZ_PLUS_1 {
                let az_count = az_counts
                    .get_mut(instance.instance_info.az.as_ref().unwrap())
                    .unwrap();
                // Have no more than two nodes per az.
                if *az_count < 2 {
                    // If this az is not yet considered, add it to the list.
                    if *az_count == 0 {
                        num_azs += 1;
                    }
                    *az_count += 1;
                    new_instances.push(instance);
                }
                // Check if replication is safe for AZ+1 failure.
                if new_instances.len() == 6 || num_azs == 5 {
                    is_safe = true;
                    break;
                }
            } else {
                // Just check that two instances are deployed.
                new_instances.push(instance);
                if new_instances.len() >= 2 {
                    is_safe = true;
                    break;
                }
            }
        }
        // If amount of replication is not safe, empty instances.
        if !is_safe {
            new_instances.clear();
        }
        let replica_urls = new_instances
            .iter()
            .map(|instance| instance.instance_info.private_url.clone().unwrap())
            .collect::<Vec<_>>();
        if USE_AZ_PLUS_1 {
            unimplemented!("IMPLEMENT ME (AZ+1)!");
        } else if !replica_urls.is_empty() {
            let (replica1, replica2) = (replica_urls[0].clone(), replica_urls[1].clone());
            tokio::task::block_in_place(|| {
                self.wal.set_replicas(Some((replica1, replica2))).unwrap();
            });
        } else {
            tokio::task::block_in_place(|| {
                self.wal.set_replicas(None).unwrap();
            });
        }
    }
}

impl WalProvider for PersistentLog {
    fn enqueue(&self, entry: Vec<u8>) -> Result<i64, low_level_systems::LLError> {
        let this = self.clone();
        std::thread::spawn(move || Ok(this.handle.block_on(this.enqueue(entry, None))))
            .join()
            .unwrap()
        // Ok(self.handle.block_on(self.enqueue(entry, None)))
    }

    fn persist(&self, lsn: Option<i64>) -> Result<(), low_level_systems::LLError> {
        let this = self.clone();
        std::thread::spawn(move || {
            this.handle.block_on(this.flush_at(lsn));
        })
        .join()
        .unwrap();
        // self.handle.block_on(self.flush_at(lsn));
        Ok(())
    }

    fn persist_lsn(&self) -> i64 {
        self.wal.persist_lsn()
    }

    fn replay_all(&self) -> Result<Vec<(Vec<u8>, i64)>, LLError> {
        let mut replay_handle = None;
        let mut entries = vec![];
        loop {
            let (new_handle, new_entries) = self.wal.replay(replay_handle)?;
            replay_handle = new_handle;
            entries.extend(new_entries);
            if replay_handle.is_none() {
                break;
            }
        }
        Ok(entries)
    }

    fn truncate(&self, lsn: i64) -> Result<(), LLError> {
        self.wal.truncate(lsn)
    }
}
