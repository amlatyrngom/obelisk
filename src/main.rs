use common;
use common::adaptation::backend::{AdapterBackend, ServiceInfo};
use common::adaptation::{AdapterScaling, Rescaler};
use common::{ActorInstance, FunctionInstance, ServiceInstance};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use lazy_static::lazy_static;
use messaging;
use messaging::MessagingHandler;
use persistence;
use serde_json::Value;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::signal::unix;
use warp::Filter;
lazy_static! {
    static ref GENERIC_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> =
        async_once::AsyncOnce::new(async {
            let subsystem = std::env::var("SUBSYSTEM").unwrap();
            let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
            let mut x: Option<Arc<dyn ServiceInstance>> = None;
            if x.is_none() && subsystem == "persistence" {
                x = Some(Arc::new(
                    persistence::LogReplica::new(svc_info.clone()).await,
                ));
            }
            Arc::new(AdapterBackend::new(svc_info, x.unwrap()).await)
        });
    static ref RESCALER: async_once::AsyncOnce<Arc<AdapterScaling>> =
        async_once::AsyncOnce::new(async {
            let subsystem = std::env::var("SUBSYSTEM").unwrap();
            let mut x: Option<Arc<dyn Rescaler>> = None;
            if x.is_none() && subsystem == "functional" {
                x = Some(Arc::new(
                    functional::rescaler::FunctionalRescaler::new().await,
                ));
            }
            if x.is_none() && subsystem == "messaging" {
                x = Some(Arc::new(messaging::MessagingRescaler::new().await));
            }
            if x.is_none() && subsystem == "persistence" {
                x = Some(Arc::new(persistence::LogRescaler::new().await));
            }
            Arc::new(AdapterScaling::new(x, &subsystem).await)
        });
    static ref FUNCTION_INSTANCE: async_once::AsyncOnce<Arc<dyn FunctionInstance>> =
        async_once::AsyncOnce::new(async {
            let namespace = std::env::var("NAMESPACE").unwrap();
            let mut x: Option<Arc<dyn FunctionInstance>> = None;
            if x.is_none() && namespace == "functional" {
                x = Some(Arc::new(functional::echo::Echo::new().await));
            }
            x.unwrap()
        });
    static ref FUNCTIONAL_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> =
        async_once::AsyncOnce::new(async {
            let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
            let invoker = Arc::new(functional::invoker::Invoker::new(svc_info.clone()).await);
            Arc::new(AdapterBackend::new(svc_info, invoker).await)
        });
    static ref MESSAGING_FUNCTION: async_once::AsyncOnce<Arc<dyn FunctionInstance>> =
        async_once::AsyncOnce::new(async {
            let namespace = std::env::var("NAMESPACE").unwrap();
            let name = std::env::var("NAME").unwrap();
            let plog = Arc::new(persistence::PersistentLog::new(&namespace, &name).await);
            let mut x: Option<Arc<dyn ActorInstance>> = None;
            if x.is_none() && namespace == "messaging" {
                x = Some(Arc::new(messaging::Echo::new(&name, plog.clone()).await));
            }
            let handler: Arc<dyn FunctionInstance> =
                Arc::new(MessagingHandler::new(plog.clone(), x.unwrap(), &namespace, &name).await);
            handler
        });
    static ref MESSAGING_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> =
        async_once::AsyncOnce::new(async {
            let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
            let namespace = svc_info.namespace.clone();
            let name = svc_info.name.clone();
            let plog = Arc::new(persistence::PersistentLog::new(&namespace, &name).await);
            let mut x: Option<Arc<dyn ActorInstance>> = None;
            if x.is_none() && namespace == "messaging" {
                x = Some(Arc::new(messaging::Echo::new(&name, plog.clone()).await));
            }
            let handler =
                Arc::new(MessagingHandler::new(plog.clone(), x.unwrap(), &namespace, &name).await);
            Arc::new(AdapterBackend::new(svc_info, handler).await)
        });
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("Main Started!");
    let mode = std::env::var("EXECUTION_MODE").unwrap();
    println!("Running mode: {mode}");
    if mode == "generic_ecs" {
        let backend = GENERIC_BACKEND.get().await.clone();
        generic_backend_main(backend).await;
        return Ok(());
    }
    if mode == "rescaler" {
        let func = service_fn(rescaler_handler);
        lambda_runtime::run(func).await?;
        return Ok(());
    }
    if mode == "functional_lambda" {
        let func = service_fn(functional_lambda_handler);
        lambda_runtime::run(func).await?;
        return Ok(());
    }
    if mode == "functional_ecs" {
        let function = FUNCTION_INSTANCE.get().await.clone();
        functional_http_handler(function).await;
        return Ok(());
    }
    if mode == "functional_backend" {
        let backend = FUNCTIONAL_BACKEND.get().await.clone();
        generic_backend_main(backend).await;
        return Ok(());
    }
    if mode == "messaging_lambda" {
        let func = lambda_runtime::service_fn(messaging_lambda_handler);
        lambda_runtime::run(func).await?;
        return Ok(());
    }
    if mode == "messaging_ecs" {
        let backend = MESSAGING_BACKEND.get().await.clone();
        generic_backend_main(backend).await;
        return Ok(());
    }
    panic!("Unknown mode!");
}
async fn graceful_wait(duration: u64) {
    let terminate_signal = unix::SignalKind::terminate();
    let mut sigterm = unix::signal(terminate_signal).unwrap();
    let sigint = tokio::signal::ctrl_c();
    tokio::select! { _ = sigterm . recv () => { println ! ("Received sigterm!") ; } _ = sigint => { println ! ("Received sigint!") ; } }
    tokio::time::sleep(std::time::Duration::from_secs(duration)).await;
}
async fn generic_backend_main(backend: Arc<AdapterBackend>) {
    let grace = std::env::var("GRACE").unwrap_or("5".into());
    let grace: u64 = grace.parse().unwrap();
    let meta = warp::header::<String>("obelisk-meta");
    let invoke_path = warp::path!("invoke")
        .and(warp::post())
        .and(meta)
        .and(warp::body::content_length_limit(1024 * 1024 * 70))
        .and(warp::body::bytes())
        .and(with_backend(backend))
        .and_then(warp_backend_handler);
    let hello_world = warp::path::end().map(|| "Hello, World at root!");
    let routes = invoke_path.or(hello_world);
    let port = common::get_port().unwrap();
    println!("Listening on port {port}");
    let listen_info = ([0, 0, 0, 0], port);
    let (_, server) =
        warp::serve(routes).bind_with_graceful_shutdown(listen_info, graceful_wait(grace));
    server.await;
    println!("Done listening on port {port}");
}
fn with_backend(
    backend: Arc<AdapterBackend>,
) -> impl Filter<Extract = (Arc<AdapterBackend>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || backend.clone())
}
pub async fn warp_backend_handler(
    meta: String,
    arg: bytes::Bytes,
    backend: Arc<AdapterBackend>,
) -> Result<impl warp::Reply, Infallible> {
    let arg: Vec<u8> = arg.to_vec();
    let (meta, resp) = backend.call_svc(meta, arg).await;
    Ok(warp::http::Response::builder()
        .status(200)
        .header("obelisk-meta", &meta)
        .body(resp)
        .unwrap())
}
async fn rescaler_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let rescaler = RESCALER.get().await.clone();
    Ok(rescaler.invoke(event).await)
}
async fn functional_http_handler(function_instance: Arc<dyn FunctionInstance>) {
    let timeout = std::env::var("TIMEOUT").unwrap_or("5".into());
    let timeout: u64 = timeout.parse().unwrap();
    let invoke_path = warp::path!("invoke")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1024 * 8))
        .and(warp::body::json())
        .and(with_function_instance(function_instance))
        .and_then(warp_functional_handler);
    let hello_world = warp::path::end().map(|| "Hello, World at root!");
    let routes = invoke_path.or(hello_world);
    let port = common::get_port().unwrap();
    println!("Listening on port {port}");
    let listen_info = ([0, 0, 0, 0], port);
    let (_, server) =
        warp::serve(routes).bind_with_graceful_shutdown(listen_info, graceful_wait(timeout));
    server.await;
    println!("Done listening on port {port}");
}
fn with_function_instance(
    function: Arc<dyn FunctionInstance>,
) -> impl Filter<Extract = (Arc<dyn FunctionInstance>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || function.clone())
}
async fn functional_lambda_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let function = FUNCTION_INSTANCE.get().await.clone();
    let start_time = std::time::Instant::now();
    let resp = function.invoke(event).await;
    let end_time = std::time::Instant::now();
    let duration_ms = end_time.duration_since(start_time).as_millis() as i64;
    let mem_size_mb = std::env::var("MEMORY").unwrap().parse().unwrap();
    let resp = common::FunctionResp {
        resp,
        duration_ms,
        mem_size_mb,
    };
    Ok(serde_json::to_value(&resp).unwrap())
}
pub async fn warp_functional_handler(
    event: Value,
    function: Arc<dyn FunctionInstance>,
) -> Result<impl warp::Reply, Infallible> {
    let start_time = std::time::Instant::now();
    let resp = function.invoke(event).await;
    let end_time = std::time::Instant::now();
    let duration_ms = end_time.duration_since(start_time).as_millis() as i64;
    let mem_size_mb = std::env::var("MEMORY").unwrap().parse().unwrap();
    let resp = common::FunctionResp {
        resp,
        duration_ms,
        mem_size_mb,
    };
    Ok(warp::reply::json(&resp))
}
async fn messaging_lambda_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let function = MESSAGING_FUNCTION.get().await.clone();
    Ok(function.invoke(event).await)
}
