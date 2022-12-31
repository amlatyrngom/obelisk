use common::FunctionInstance;
use serde_json::Value;

pub struct Echo {}

impl Echo {
    pub async fn new() -> Self {
        Echo {}
    }
}

#[async_trait::async_trait]
impl FunctionInstance for Echo {
    async fn invoke(&self, arg: Value) -> Value {
        if let Some(sleep_duration) = arg.as_u64() {
            tokio::time::sleep(std::time::Duration::from_millis(sleep_duration)).await;
        }
        arg
    }
}
