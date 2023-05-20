use common::FunctionInstance;
use serde_json::Value;

pub struct MicroFunction {}

impl MicroFunction {
    pub async fn new() -> Self {
        MicroFunction {}
    }
}

#[async_trait::async_trait]
impl FunctionInstance for MicroFunction {
    async fn invoke(&self, arg: Value) -> Value {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        arg
    }
}
