use crate::{clean_die, time_service::TimeService};
use aws_sdk_dynamodb::types::AttributeValue;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Within is time period, no need to recheck state store.
const LEASE_GUARANTEE_DURATION: i64 = 18;
/// After this time period without renewal, the lease is lost.
const LEASE_GRACE_DURATION: i64 = 30;
/// Lease will be refreshed in the background if owned for this long.
/// Keep this lower than `LEASE_GUARANTEE_DURATION - 1`.
const LEASE_REFRESH_DURATION: i64 = 12;

/// Tracks ownership of leases locally.
#[derive(Clone, Debug)]
struct OwnershipInfo {
    timestamp: chrono::DateTime<chrono::Utc>,
    owns: bool,
}

/// Leaser.
/// Uses NTP to keep time difference accross nodes reasonable.
///
/// Call `renew()` to renew leases.
/// When it succeeds, then for the next `LEASE_GUARANTEE_DURATION` seconds,
/// the lease is highly likely to be valid, since other nodes have to wait
/// `LEASE_GRACE_DURATION` before acquiring the lease.
///
/// However, to get 100% guarantee, you need to add `lease_check()` to your txns.
///
/// Call `revoke()` to revoke lease if owned.
#[derive(Clone)]
pub struct Leaser {
    dynamo_client: aws_sdk_dynamodb::Client,
    lease_id: String,
    table_name: String,
    pub time_service: TimeService,
    inner: Arc<Mutex<LeaserInner>>,
}

/// For inner modification.
struct LeaserInner {
    ownership: HashMap<String, OwnershipInfo>,
}

impl LeaserInner {
    /// Create.
    async fn new() -> Self {
        let ownership = HashMap::new();
        LeaserInner { ownership }
    }

    /// Check local state of lease. Returns (`own_time`, `exists`).
    /// `own_time` is Some(time) locally owned and if time is within guaranteed duration.
    /// Otherwise it's none.
    /// `exists` if lease is locally cached state within guaranteed duration.
    /// If exists=true and own_time=None, then assume not owned.
    /// If exists=false and own_time=None, then make request to dynamo.
    async fn local_check(
        &mut self,
        name: &str,
        curr_time: &chrono::DateTime<chrono::Utc>,
        grace_time: &chrono::DateTime<chrono::Utc>,
    ) -> (Option<chrono::DateTime<chrono::Utc>>, bool) {
        // Cleanup outdated leases.
        let mut to_delete = Vec::new();
        for (k, info) in self.ownership.iter() {
            if info.timestamp < *grace_time {
                to_delete.push(k.clone());
            }
        }
        for k in to_delete {
            self.ownership.remove(&k);
        }
        if !self.ownership.contains_key(name) {
            (None, false)
        } else {
            let info = self.ownership.get(name).unwrap();
            let since = curr_time.signed_duration_since(info.timestamp);
            let owned = info.owns && since < chrono::Duration::seconds(LEASE_GUARANTEE_DURATION);
            let not_owned =
                !info.owns && since < chrono::Duration::seconds(LEASE_GUARANTEE_DURATION);
            if owned {
                (Some(info.timestamp), true)
            } else if not_owned {
                (None, true)
            } else {
                (None, false)
            }
        }
    }

    /// Mark ownership.
    async fn update_ownership(
        &mut self,
        name: &str,
        curr_time: &chrono::DateTime<chrono::Utc>,
        owns: bool,
    ) {
        let info = OwnershipInfo {
            owns,
            timestamp: *curr_time,
        };
        self.ownership.insert(name.into(), info);
    }
}

impl Leaser {
    /// Create a new leaser.
    pub async fn new(table_name: &str) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let inner = Arc::new(Mutex::new(LeaserInner::new().await));
        let lease_id = Uuid::new_v4().to_string();
        Leaser {
            dynamo_client,
            lease_id,
            inner,
            table_name: table_name.into(),
            time_service: TimeService::new().await,
        }
    }

    /// Get current time and grace time.
    /// Will periodically refresh ntp time to prevent excessive drift.
    async fn get_time_interval(
        &self,
    ) -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
        let curr_time = self.time_service.current_time().await;
        let grace_time = curr_time
            .checked_sub_signed(chrono::Duration::seconds(LEASE_GRACE_DURATION))
            .unwrap();
        (curr_time, grace_time)
    }

    /// Returns the id of this lease.
    /// This id is guaranteed to be unique.
    /// Use it to identify your node.
    pub fn lease_id(&self) -> String {
        self.lease_id.clone()
    }

    /// Returns reasonably well synchronized current time partly using NTP.
    pub async fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.time_service.current_time().await
    }

    /// Revoke lease if owned.
    pub async fn revoke(&self, name: &str) -> bool {
        let mut inner = self.inner.lock().await;
        inner.ownership.remove(name);
        let resp = self
            .dynamo_client
            .delete_item()
            .table_name(&self.table_name)
            .key("namespace", AttributeValue::S("system".into()))
            .key("name", AttributeValue::S(name.into()))
            .condition_expression("attribute_not_exists(#namespace) OR lease_id = :lease_id")
            .expression_attribute_names("#namespace", "namespace")
            .expression_attribute_values(":lease_id", AttributeValue::S(self.lease_id.clone()))
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}").to_lowercase();
            if err.contains("condition") {
                false
            } else {
                // We don't allow dynamo errors.
                // Just die when such an error occurs. In a cloud environment, there will be a restart.
                return clean_die(&format!("Unhandled dynamo error: {err}")).await;
            }
        } else {
            true
        }
    }

    /// Delete a lease, bypassing any ownership.
    /// Use this only when you are certain no one is using the lease.
    /// Otherwise let GC handle unused leases.
    pub async fn delete(&self, name: &str) {
        let mut inner = self.inner.lock().await;
        inner.ownership.remove(name);
        self.dynamo_client
            .delete_item()
            .table_name(&self.table_name)
            .key("namespace", AttributeValue::S("system".into()))
            .key("name", AttributeValue::S(name.into()))
            .send()
            .await
            .unwrap();
    }

    /// Renew a lease.
    /// Setting `bypass_local` is useful to avoid delays after revoke() by another node.
    pub async fn renew(&self, name: &str, bypass_local: bool) -> bool {
        // log::debug!("Lease ({}).", self.lease_id);
        let (curr_time, grace_time) = {
            let mut inner = self.inner.lock().await;
            let (curr_time, grace_time) = self.get_time_interval().await;
            // log::debug!(
            //     "Lease ({}). Time: ({curr_time:?}, {grace_time:?})",
            //     self.lease_id
            // );
            let (owns, exists) = inner.local_check(name, &curr_time, &grace_time).await;
            if exists && owns.is_some() {
                let this = self.clone();
                let name = name.to_string();
                let since = owns.unwrap().signed_duration_since(curr_time);
                if since < chrono::Duration::seconds(LEASE_REFRESH_DURATION) {
                    tokio::spawn(async move {
                        this.lease_request(&name, &curr_time, &grace_time).await;
                    });
                }
                return true;
            }
            if !bypass_local && exists && owns.is_none() {
                return false;
            }
            (curr_time, grace_time)
        };
        // log::debug!("Lease ({}). Making request.", self.lease_id);
        self.lease_request(name, &curr_time, &grace_time).await
    }

    /// Make lease acquisition request to dynamodb.
    async fn lease_request(
        &self,
        name: &str,
        curr_time: &chrono::DateTime<chrono::Utc>,
        grace_time: &chrono::DateTime<chrono::Utc>,
    ) -> bool {
        // This delay is just for GC for when the lease is not accessed after a while.
        let delay = chrono::Duration::minutes(10);
        let gc_time = curr_time.checked_add_signed(delay).unwrap();
        let gc_time = gc_time.timestamp();
        // Lease content:
        // lease_id: id of lease.
        // value: same as lease_id to allow user to read lease.
        println!("Writing lease.");
        let resp = self
            .dynamo_client
            .put_item()
            .table_name(&self.table_name)
            .item("subsystem", AttributeValue::S("leasing".into()))
            .item("identifier", AttributeValue::S(name.into()))
            .item("lease_id", AttributeValue::S(self.lease_id.clone()))
            .item("value", AttributeValue::S(self.lease_id.clone()))
            .item(
                "lease_time",
                AttributeValue::N(curr_time.timestamp().to_string()),
            )
            .item("gc_ttl", AttributeValue::N(gc_time.to_string()))
            .condition_expression(
                "attribute_not_exists(#namespace) OR lease_id = :lease_id OR lease_time < :grace_time",
            )
            .expression_attribute_names("#namespace", "namespace")
            .expression_attribute_values(":lease_id", AttributeValue::S(self.lease_id.clone()))
            .expression_attribute_values(
                ":grace_time",
                AttributeValue::N(grace_time.timestamp().to_string()),
            )
            .send()
            .await;
        // Owns if conditional put succeeds.
        let owns = if resp.is_err() {
            let err = format!("{resp:?}");
            if err.contains("ConditionalCheckFailedException") {
                println!("Lease condition fail: {err:?}");
                false
            } else {
                // We don't allow arbitrary dynamo errors.
                // Just die when such an error occurs. In a cloud environment, there will be a restart.
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        } else {
            true
        };
        let mut inner = self.inner.lock().await;
        // log::debug!("Lease ({}). Ownership (owns={owns}, curr_time={curr_time:?}, grace_time={grace_time:?})", self.lease_id);
        inner.update_ownership(name, curr_time, owns).await;
        owns
    }
}

#[cfg(test)]
mod tests {
    use super::Leaser;
    use super::{LEASE_GRACE_DURATION, LEASE_GUARANTEE_DURATION};
    use aws_sdk_dynamodb::types::{AttributeDefinition, ScalarAttributeType};
    use aws_sdk_dynamodb::types::{
        BillingMode, KeySchemaElement, KeyType, TimeToLiveSpecification,
    };
    const TEST_LEASING_TABLE: &str = "obk__test__leasing";

    /// Create table for test.
    async fn make_test_table() {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        // key-value table.
        let resp = dynamo_client
            .create_table()
            .table_name(TEST_LEASING_TABLE)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("namespace")
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("name")
                    .key_type(KeyType::Range)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("namespace")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("name")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            if !err.contains("ResourceInUseException") {
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        } else {
            println!("Creating {TEST_LEASING_TABLE} table...");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
        let resp = dynamo_client
            .update_time_to_live()
            .table_name(TEST_LEASING_TABLE)
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("gc_ttl")
                    .enabled(true)
                    .build(),
            )
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            if !err.contains("TimeToLive is already enabled") {
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        }
    }

    /// Basic, comprehensive test.
    async fn run_basic_test(key: &str) {
        let leaser1 = Leaser::new(TEST_LEASING_TABLE).await;
        let leaser2 = Leaser::new(TEST_LEASING_TABLE).await;
        // Check that ids are truly different.
        assert_ne!(leaser1.lease_id(), leaser2.lease_id());
        // Check that clock drift is reasonable.
        // This is not guaranteed to work, but it very very likely should.
        let time1 = leaser1.current_time().await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(time1 < leaser1.current_time().await);
        assert!(time1 < leaser2.current_time().await);
        // Init: delete.
        leaser1.delete(key).await;
        // Test 1: renew1(y), renew2(n), bypass_renew2(n).
        assert!(leaser1.renew(key, false).await);
        assert!(!leaser2.renew(key, false).await);
        assert!(!leaser2.renew(key, true).await);
        // Test 2: short_sleep, renew2(n), bypass_renew2(n), renew1(y).
        tokio::time::sleep(std::time::Duration::from_secs(
            (LEASE_GUARANTEE_DURATION - 1) as u64,
        ))
        .await;
        assert!(!leaser2.renew(key, false).await);
        assert!(!leaser2.renew(key, true).await);
        assert!(leaser1.renew(key, false).await);
        // Test 3: medium_sleep, renew2(n), renew1(y), revoke1(y), renew2(n), bypass_renew2(y)
        tokio::time::sleep(std::time::Duration::from_secs(
            (LEASE_GUARANTEE_DURATION + 1) as u64,
        ))
        .await;
        assert!(!leaser2.renew(key, false).await);
        assert!(leaser1.renew(key, false).await);
        assert!(!leaser2.renew(key, false).await);
        assert!(leaser1.revoke(key).await);
        assert!(!leaser2.renew(key, false).await);
        assert!(leaser2.renew(key, true).await);
        // Test 4: medium_sleep, renew2(y), revoke1(n), revoke2(y), renew1(y).
        tokio::time::sleep(std::time::Duration::from_secs(
            (LEASE_GUARANTEE_DURATION + 1) as u64,
        ))
        .await;
        assert!(leaser2.renew(key, false).await);
        assert!(!leaser1.revoke(key).await);
        assert!(leaser2.revoke(key).await);
        assert!(leaser1.renew(key, false).await);
        // Test 5: medium_sleep, renew1(y), renew2(n), long_sleep, renew2(true), renew1(n).
        tokio::time::sleep(std::time::Duration::from_secs(
            (LEASE_GUARANTEE_DURATION + 1) as u64,
        ))
        .await;
        assert!(leaser1.renew(key, false).await);
        assert!(!leaser2.renew(key, false).await);
        tokio::time::sleep(std::time::Duration::from_secs(
            (LEASE_GRACE_DURATION + 1) as u64,
        ))
        .await;
        assert!(leaser2.renew(key, false).await);
        assert!(!leaser1.renew(key, false).await);
    }

    #[tokio::test]
    async fn test_leasing() {
        make_test_table().await;
        // Set task count to 1 for debugging.
        let task_count = 1;
        let mut tasks = Vec::new();
        for i in 0..task_count {
            let key = format!("test_key{i}");
            tasks.push(tokio::spawn(async move {
                run_basic_test(&key).await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }
}
