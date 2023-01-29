use rsntp::AsyncSntpClient;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
/// Keep >= 10. If not, some time servers may flag you as ddos attacker.
const NTP_REFRESH: i64 = 10;

#[derive(Clone)]
pub struct TimeService {
    inner: Arc<Mutex<TimeServiceInner>>,
}

struct TimeServiceInner {
    last_ntp_time: chrono::DateTime<chrono::Utc>,
    last_ntp_req: std::time::Instant,
    sntp_client: AsyncSntpClient,
}

impl TimeService {
    /// Create.
    pub async fn new() -> Self {
        let past = Instant::now()
            .checked_sub(std::time::Duration::from_secs(1000))
            .unwrap();
        let mut sntp_client = AsyncSntpClient::new();
        sntp_client.set_timeout(std::time::Duration::from_secs(NTP_REFRESH as u64));
        let inner = Arc::new(Mutex::new(TimeServiceInner {
            sntp_client,
            last_ntp_req: past,
            last_ntp_time: chrono::Utc::now(),
        }));
        TimeService { inner }
    }

    /// Get current time
    pub async fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let since_req = now.duration_since(inner.last_ntp_req);
        let since_req = chrono::Duration::from_std(since_req).unwrap();
        let curr_time = if since_req < chrono::Duration::seconds(NTP_REFRESH) {
            inner.last_ntp_time.checked_add_signed(since_req).unwrap()
        } else {
            let ntp_time = loop {
                inner.last_ntp_req = Instant::now();
                if crate::has_external_access() {
                    let res = inner.sntp_client.synchronize("time.google.com").await;
                    if let Ok(res) = res {
                        break res.datetime().into_chrono_datetime().unwrap();
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                } else {
                    // In lambda mode, just use local time.
                    println!("Warning: using local time due to absence of external access!");
                    break chrono::Utc::now();
                }
                
            };
            inner.last_ntp_time = ntp_time;
            inner.last_ntp_time
        };
        curr_time
    }
}
