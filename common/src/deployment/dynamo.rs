use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, TableStatus, TimeToLiveSpecification,
};

use super::NamespaceSpec;

pub struct DynamoDeployment {}

impl DynamoDeployment {
    /// Name of the scaling table.
    pub fn scaling_table_name(namespace: &str) -> String {
        format!("obk__scaling_state__{namespace}")
    }

    /// Create scaling table.
    pub async fn create_scaling_table(
        client: &aws_sdk_dynamodb::Client,
        namespace: &str,
    ) -> Result<(), String> {
        let scaling_table = Self::scaling_table_name(namespace);
        loop {
            let resp = client
                .describe_table()
                .table_name(&scaling_table)
                .send()
                .await
                .map_err(|e| format!("{e:?}"));
            match resp {
                Ok(resp) => {
                    let table = resp.table().unwrap();
                    let status = table.table_status().unwrap();
                    match status {
                        TableStatus::Active => break,
                        _ => continue,
                    }
                }
                Err(e) => {
                    if !e.contains("ResourceNotFoundException") {
                        return Err(e);
                    }
                }
            };
            // Create table.
            let resp = client
                .create_table()
                .table_name(&scaling_table)
                .billing_mode(BillingMode::PayPerRequest)
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("subsystem")
                        .key_type(KeyType::Hash)
                        .build(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("identifier")
                        .key_type(KeyType::Range)
                        .build(),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("subsystem")
                        .attribute_type(ScalarAttributeType::S)
                        .build(),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("identifier")
                        .attribute_type(ScalarAttributeType::S)
                        .build(),
                )
                .send()
                .await
                .map_err(|e| format!("{e:?}"));
            match resp {
                Ok(_) => continue,
                Err(e) => {
                    if !e.contains("ResourceInUseException") {
                        return Err(e);
                    } else {
                        continue;
                    }
                }
            }
        }
        // Set TTL.
        let resp = client
            .update_time_to_live()
            .table_name(&scaling_table)
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("gc_ttl")
                    .enabled(true)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| format!("{e:?}"));
        match resp {
            Ok(_) => Ok(()),
            Err(e) => {
                if !e.contains("TimeToLive is already enabled") {
                    Err(e)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Write the namespace of a spec.
    pub async fn write_namespace_spec(
        client: &aws_sdk_dynamodb::Client,
        namespace_spec: &NamespaceSpec,
    ) -> Result<(), String> {
        let table_name = Self::scaling_table_name(&namespace_spec.namespace);
        let spec = serde_json::to_string(namespace_spec).unwrap();
        let revision = uuid::Uuid::new_v4().to_string();
        client
            .put_item()
            .table_name(&table_name)
            .item("subsystem", AttributeValue::S("system".into()))
            .item("identifier", AttributeValue::S("system".into()))
            .item("spec", AttributeValue::S(spec))
            .item("revision", AttributeValue::S(revision))
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    /// Fetch the spec of a namespace.
    pub async fn fetch_namespace_spec(
        client: &aws_sdk_dynamodb::Client,
        namespace: &str,
    ) -> Result<(NamespaceSpec, String), String> {
        let table_name = Self::scaling_table_name(&namespace);
        let item = client
            .get_item()
            .table_name(&table_name)
            .key("subsystem", AttributeValue::S("system".into()))
            .key("identifier", AttributeValue::S("system".into()))
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let item = item.item().ok_or("Namespace not found")?;
        let spec = item.get("spec").unwrap().as_s().unwrap();
        let spec: NamespaceSpec = serde_json::from_str(spec).unwrap();
        let revision = item.get("revision").unwrap().as_s().unwrap();
        Ok((spec, revision.clone()))
    }
}
