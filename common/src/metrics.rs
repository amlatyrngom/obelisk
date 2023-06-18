use std::sync::Arc;
use tokio::signal::unix;
use tokio::sync::Mutex;

/// Amount of time to push metrics.
pub const METRIC_PUSH_INTERVAL_SECS: u64 = 10;

/// Manages metrics.
#[derive(Clone, Debug)]
pub struct MetricsManager {
    /// Subsystem doing the scaling.
    subsystem: String,
    /// Namespace of the caller.
    namespace: String,
    /// Identifier of the caller.
    identifier: String,
    /// S3 client.
    s3_client: aws_sdk_s3::Client,
    /// Modifyable inner.
    inner: Arc<Mutex<MetricsManagerInner>>,
}

/// Modifyable Inner.
#[derive(Clone, Debug)]
struct MetricsManagerInner {
    curr_metrics: Vec<Vec<u8>>,
}

impl MetricsManager {
    /// Name of the metrics queue.
    pub fn metrics_bucket_name() -> String {
        crate::bucket_name()
    }

    /// Name of the metrics directory.
    pub fn metrics_dir_name(subsystem: &str, namespace: &str, identifier: &str) -> String {
        format!(
            "{}/metrics/{subsystem}_{namespace}_{identifier}",
            crate::tmp_s3_dir()
        )
    }

    /// Key to store new metrics.
    pub fn new_metrics_key(subsystem: &str, namespace: &str, identifier: &str) -> String {
        format!(
            "{}/{}",
            Self::metrics_dir_name(subsystem, namespace, identifier),
            uuid::Uuid::new_v4().to_string()
        )
    }

    /// Create.
    pub async fn new(subsystem: &str, namespace: &str, identifier: &str) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let inner = Arc::new(Mutex::new(MetricsManagerInner {
            curr_metrics: Vec::new(),
        }));
        MetricsManager {
            subsystem: subsystem.into(),
            namespace: namespace.into(),
            identifier: identifier.into(),
            inner,
            s3_client,
        }
    }

    /// Start metrics pushing thread
    pub async fn start_metrics_pushing_thread(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.metrics_pushing_thread().await;
        });
    }

    /// Periodically push metrics.
    async fn metrics_pushing_thread(&self) {
        let push_duration = std::time::Duration::from_secs(METRIC_PUSH_INTERVAL_SECS);
        let mut push_interval = tokio::time::interval(push_duration);
        push_interval.tick().await;
        let terminate_signal = unix::SignalKind::terminate();
        let interrupt_signal = unix::SignalKind::interrupt();
        let mut sigterm = unix::signal(terminate_signal).unwrap();
        let mut sigint = unix::signal(interrupt_signal).unwrap();
        loop {
            tokio::select! {
                _ = push_interval.tick() => {
                    let _ = self.push_metrics().await;
                },
                _ = sigint.recv() => {
                    let _ = self.push_metrics().await;
                    return;
                }
                _ = sigterm.recv() => {
                    let _ = self.push_metrics().await;
                    return;
                }
            }
        }
    }

    /// Accumulate metrics in local vector.
    pub async fn accumulate_metric(&self, metric: Vec<u8>) {
        let mut inner = self.inner.lock().await;
        inner.curr_metrics.push(metric);
    }

    /// Push accumulated metrics.
    /// Call this infrequently (e.g., every 5-10 seconds)
    pub async fn push_metrics(&self) -> Result<(), String> {
        let metrics = {
            let mut inner = self.inner.lock().await;
            inner.curr_metrics.drain(..).collect::<Vec<_>>()
        };
        if metrics.is_empty() {
            return Ok(());
        }
        let metrics = bincode::serialize(&metrics).unwrap();
        let body = aws_sdk_s3::primitives::ByteStream::from(metrics);
        let key = Self::new_metrics_key(&self.subsystem, &self.namespace, &self.identifier);
        let _resp = self
            .s3_client
            .put_object()
            .bucket(Self::metrics_bucket_name())
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    /// Retrieve stored metrics.
    /// Returns (metrics_keys, metrics)
    pub async fn retrieve_metrics(&self) -> Result<(Vec<String>, Vec<Vec<u8>>), String> {
        // Scan metrics keys.
        let s3_prefix = Self::metrics_dir_name(&self.subsystem, &self.namespace, &self.identifier);
        let resp = self
            .s3_client
            .list_objects_v2()
            .bucket(Self::metrics_bucket_name())
            .prefix(s3_prefix)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let objs = resp.contents().unwrap_or_else(|| &[]);
        let keys = objs
            .iter()
            .map(|obj| obj.key().unwrap().to_string())
            .collect::<Vec<_>>();
        // Read each object and deserialize it.
        let mut s3_reads = Vec::new();
        for key in &keys {
            let key = key.clone();
            let s3_client = self.s3_client.clone();
            s3_reads.push(tokio::spawn(async move {
                let resp = s3_client
                    .get_object()
                    .bucket(&Self::metrics_bucket_name())
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| format!("{e:?}"));
                let resp = match resp {
                    Err(e) => Err(e),
                    Ok(resp) => {
                        let body = resp.body.collect().await.unwrap();
                        let body = body.into_bytes().to_vec();
                        let body: Vec<Vec<u8>> = bincode::deserialize(&body).unwrap();
                        Ok(body)
                    }
                };
                resp
            }));
        }
        let mut metrics = Vec::new();
        for s3_read in s3_reads {
            let resp = s3_read.await.map_err(|e| format!("{e:?}"))?;
            let resp = resp?;
            metrics.extend(resp);
        }
        Ok((keys, metrics))
    }

    /// Delete processed metrics.
    pub async fn delete_metrics(&self, metrics_keys: &[String]) -> Result<(), String> {
        // Delete responses.
        let mut s3_deletes = Vec::new();
        for key in metrics_keys {
            let key = key.clone();
            let s3_client = self.s3_client.clone();
            s3_deletes.push(tokio::spawn(async move {
                let resp = s3_client
                    .delete_object()
                    .bucket(&Self::metrics_bucket_name())
                    .key(&key)
                    .send()
                    .await;
                resp.map_err(|e| format!("{e:?}"))
            }));
        }
        // Wait for writes.
        for t in s3_deletes {
            let resp = t.await.map_err(|e| format!("{e:?}"))?;
            let _resp = resp?;
        }
        Ok(())
    }
}
