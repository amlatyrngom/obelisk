pub mod client;
pub mod echo;
pub mod invoker;
pub mod rescaler;

pub use client::FunctionalClient;

/// Return a list of deployment specs.
/// Most likely, you only need one.
pub async fn prepare_deployment() -> Vec<String> {
    // Return spec
    // let spec = include_str!("deployment.toml");
    let spec = include_str!("essai.toml");
    vec![spec.into()]
}
