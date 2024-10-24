# VISC (internally named `obelisk`)
## Setup
1. Configure your environment to contain your AWS credentials. You should have full access to services in `common/src/deployment/full_access_policy.json`.
2. Run `cargo run -p deployment` to deploy the stack to AWS.
3. Run one of the tests. For example: `cargo test --package functional --lib -- client::tests::simple_fn_invoke_test --exact --show-output`

## Creating a new function or actor
Checkout `functional/src/echo.rs` and `functional/src/deployment.toml` for creating and configuring a new function or actor.
Also checkout the tests in `functional/src/client.rs` for examples on how to invoke the function or actor.


## Creating a new building block
Checkout `persistence/src/deployment.toml` for creating a new building block. It contains the WAL example.