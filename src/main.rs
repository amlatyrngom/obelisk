use common;
use common::{InstanceInfo, ServerlessHandler, ServerlessWrapper};
use common::{Rescaler, RescalerSpec, ScalingStateRescaler};
use jemallocator::Jemalloc;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use lazy_static::lazy_static;
use serde_json::Value;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::signal::unix;
use warp::Filter;
lazy_static! {
    static ref RESCALER: async_once::AsyncOnce<Arc<ScalingStateRescaler>> =
        async_once::AsyncOnce::new(async {
            let spec = std::env::var("OBK_RESCALER_SPEC").unwrap();
            let spec: RescalerSpec = serde_json::from_str(&spec).unwrap();
            let namespace = spec.namespace;
            let mut x: Option<Arc<dyn Rescaler>> = None;
            if x.is_none() && namespace == "functional" {
                x = Some(Arc::new(
                    functional::rescaler::FunctionalRescaler::new().await,
                ));
            }
            Arc::new(ScalingStateRescaler::new(&namespace, x.unwrap()).await)
        });
    static ref HANDLER_WRAPPER: async_once::AsyncOnce<Arc<ServerlessWrapper>> =
        async_once::AsyncOnce::new(async {
            let instance_info = Arc::new(InstanceInfo::new().await.unwrap());
            let serverless_storage = ServerlessStorage::new_from_info(instance_info.clone())
                .await
                .unwrap();
            let (namespace, name) = if let Some(service_name) = &instance_info.service_name {
                (&instance_info.subsystem, service_name)
            } else {
                (
                    &instance_info.namespace,
                    instance_info.handler_name.as_ref().unwrap(),
                )
            };
            let mut x: Option<Arc<dyn ServerlessHandler>> = None;
            if x.is_none() && namespace == "functional" && name == "invoker" {
                let kit = HandlerKit {
                    instance_info: instance_info.clone(),
                    serverless_storage: serverless_storage.clone(),
                };
                x = Some(Arc::new(
                    functional::invoker::Invoker::new(instance_info.clone()).await,
                ));
            }
            if x.is_none() && namespace == "functional" && name == "echofn" {
                let kit = HandlerKit {
                    instance_info: instance_info.clone(),
                    serverless_storage: serverless_storage.clone(),
                };
                x = Some(Arc::new(functional::echo::Echo::new(kit).await));
            }
            Arc::new(ServerlessWrapper::new(instance_info, serverless_storage, x.unwrap()).await)
        });
}
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("Main Started!");
    let mode = std::env::var("OBK_EXECUTION_MODE").unwrap();
    println!("Running mode: {mode}");
    if mode == "rescaler_lambda" {
        let func = service_fn(rescaler_handler);
        lambda_runtime::run(func).await?;
        return Ok(());
    }
    if mode.contains("lambda") {
        let func = service_fn(lambda_wrapper);
        lambda_runtime::run(func).await?;
        return Ok(());
    }
    if mode.contains("ecs") {
        wrapper_main().await;
        return Ok(());
    }
    panic!("Unknown mode!");
}
async fn rescaler_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let rescaler = RESCALER.get().await.clone();
    let resp = rescaler.handle_lambda_request(event.clone()).await;
    match resp {
        Ok(resp) => {
            println!("Rescaling ({event:?}) Results: {resp:?}");
            Ok(resp)
        }
        Err(e) => {
            panic!("Rescaling Erro");
        }
    }
}
async fn lambda_wrapper(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let wrapper = HANDLER_WRAPPER.get().await.clone();
    Ok(wrapper.handle_lambda_message(event).await)
}
async fn graceful_wait(duration_secs: u64) {
    let terminate_signal = unix::SignalKind::terminate();
    let mut sigterm = unix::signal(terminate_signal).unwrap();
    let sigint = tokio::signal::ctrl_c();
    tokio::select! { _ = sigterm . recv () => { println ! ("Received sigterm!") ; } _ = sigint => { println ! ("Received sigint!") ; } }
    tokio::time::sleep(std::time::Duration::from_secs(duration_secs)).await;
}
async fn wrapper_main() {
    let wrapper = HANDLER_WRAPPER.get().await.clone();
    let meta = warp::header::<String>("obelisk-meta");
    let invoke_path = warp::path!("invoke")
        .and(warp::post())
        .and(meta)
        .and(warp::body::content_length_limit(1024 * 1024 * 1024))
        .and(warp::body::bytes())
        .and(with_wrapper(wrapper))
        .and_then(warp_handler);
    let hello_world = warp::path::end().map(|| "Hello, World at root!");
    let routes = invoke_path.or(hello_world);
    let port = common::get_port();
    println!("Listening on port {port}");
    let listen_info = ([0, 0, 0, 0], port);
    let (_, server) =
        warp::serve(routes).bind_with_graceful_shutdown(listen_info, graceful_wait(60));
    server.await;
    println!("Done listening on port {port}");
}
fn with_wrapper(
    wrapper: Arc<ServerlessWrapper>,
) -> impl Filter<Extract = (Arc<ServerlessWrapper>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || wrapper.clone())
}
pub async fn warp_handler(
    meta: String,
    payload: bytes::Bytes,
    wrapper: Arc<ServerlessWrapper>,
) -> Result<impl warp::Reply, Infallible> {
    let payload: Vec<u8> = payload.to_vec();
    let (resp_meta, resp_payload) = wrapper.handle_ecs_message(meta, payload).await;
    Ok(warp::http::Response::builder()
        .status(200)
        .header("obelisk-meta", &resp_meta)
        .body(resp_payload)
        .unwrap())
}
