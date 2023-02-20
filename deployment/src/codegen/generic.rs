use super::Deployment;
use proc_macro2::TokenStream;
use quote::quote;

pub fn make_system_path(path: &str, for_system: bool) -> syn::Path {
    let path = if for_system {
        path.into()
    } else {
        format!("obelisk::{path}")
    };
    syn::parse_str(&path).unwrap()
}

pub fn gen_imports(for_system: bool) -> TokenStream {
    let common_path = make_system_path("common", for_system);
    let messaging_path = make_system_path("messaging", for_system);
    let persistence_path = make_system_path("persistence", for_system);
    let imports = quote! {
        use lambda_runtime::{service_fn, LambdaEvent, Error};
        use serde_json::Value;
        use #common_path;
        use #common_path::{FunctionInstance, ServiceInstance, ActorInstance};
        use #common_path::adaptation::{AdapterScaling, Rescaler};
        use #common_path::adaptation::backend::{AdapterBackend, ServiceInfo};
        use #messaging_path;
        use #messaging_path::MessagingHandler;
        use #persistence_path;
        use lazy_static::lazy_static;
        use std::sync::Arc;
        use warp::Filter;
        use std::convert::Infallible;
        use tokio::signal::unix;
    };

    imports
}

pub fn gen_generic_static_code(deployments: &[Deployment]) -> TokenStream {
    let mut all_generic_checks = quote! {};
    for deployment in deployments {
        if deployment.namespace != "messaging" && deployment.namespace != "functional" {
            if let Some(subsystem) = &deployment.subsystem {
                let subsystem_name = &deployment.namespace;
                // Add service check.
                let path: syn::Path = syn::parse_str(&subsystem.service_path).unwrap();
                all_generic_checks = quote! {
                    #all_generic_checks
                    if x.is_none() && subsystem == #subsystem_name {
                        x = Some(Arc::new(#path::new(svc_info.clone()).await));
                    }
                };
            }
        }
    }

    let result = quote! {
        static ref GENERIC_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> = async_once::AsyncOnce::new(async {
            let subsystem = std::env::var("SUBSYSTEM").unwrap();
            let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
            let mut x: Option<Arc<dyn ServiceInstance>> = None;
            #all_generic_checks
            Arc::new(AdapterBackend::new(svc_info, x.unwrap()).await)
        });
    };

    result
}

pub fn gen_generic_main() -> TokenStream {
    quote! {
        if mode == "generic_ecs" {
            let backend = GENERIC_BACKEND.get().await.clone();
            generic_backend_main(backend).await;
            return Ok(());
        }
    }
}

pub fn gen_generic_aux() -> TokenStream {
    quote! {
        async fn graceful_wait(duration: u64) {
            let terminate_signal = unix::SignalKind::terminate();
            let mut sigterm = unix::signal(terminate_signal).unwrap();
            let sigint = tokio::signal::ctrl_c();
            tokio::select! {
                _ = sigterm.recv() => {
                    println!("Received sigterm!");
                }
                _ = sigint => {
                    println!("Received sigint!");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(duration)).await;
        }

        async fn generic_backend_main(backend: Arc<AdapterBackend>) {
            let grace = std::env::var("GRACE").unwrap_or("5".into());
            let grace: u64 = grace.parse().unwrap();
            let meta = warp::header::<String>("obelisk-meta");
            let invoke_path = warp::path!("invoke")
                .and(warp::post())
                .and(meta)
                .and(warp::body::content_length_limit(1024 * 1024 * 1024)) // 1GB.
                .and(warp::body::bytes())
                .and(with_backend(backend))
                .and_then(warp_backend_handler);
            let hello_world = warp::path::end().map(|| "Hello, World at root!");
            let routes = invoke_path.or(hello_world);
            let port = common::get_port().unwrap();
            println!("Listening on port {port}");
            let listen_info = ([0, 0, 0, 0], port);
            let (_, server) = warp::serve(routes).bind_with_graceful_shutdown(
                listen_info,
                graceful_wait(grace),
            );
            server.await;
            println!("Done listening on port {port}");
        }

        fn with_backend(backend: Arc<AdapterBackend>) -> impl Filter<Extract = (Arc<AdapterBackend>,), Error = std::convert::Infallible> + Clone {
            warp::any().map(move || backend.clone())
        }

        pub async fn warp_backend_handler(meta: String, arg: bytes::Bytes, backend: Arc<AdapterBackend>) -> Result<impl warp::Reply, Infallible> {
            let arg: Vec<u8> = arg.to_vec();
            let (meta, resp) = backend.call_svc(meta, arg).await;
            Ok(warp::http::Response::builder()
                .status(200)
                .header("obelisk-meta", &meta)
                .body(resp)
                .unwrap()
            )
        }
    }
}
