#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut specs: Vec<String> = vec![];
    specs.extend(functional::prepare_deployment().await.into_iter());
    specs.extend(messaging::prepare_deployment().await.into_iter());
    specs.extend(persistence::prepare_deployment().await.into_iter());
    specs.extend(microbench::prepare_deployment().await.into_iter());
    deployment::build_system(&specs).await;
}
