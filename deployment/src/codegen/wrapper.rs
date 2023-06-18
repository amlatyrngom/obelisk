use super::Deployment1;
use proc_macro2::TokenStream;
use quote::quote;

pub fn gen_wrapper_static_code(deployments: &[Deployment1]) -> TokenStream {
    let mut all_handler_checks = quote! {};
    // Add services and rescalers.
    for deployment in deployments.iter() {
        let namespace = &deployment.namespace;
        if let Some(subsystem) = &deployment.subsystem {
            for service in &subsystem.services {
                // Add scaler check.
                let path: syn::Path = syn::parse_str(&service.path).unwrap();
                let name = &service.name;
                all_handler_checks = quote! {
                    #all_handler_checks
                    if x.is_none() && namespace == #namespace && name == #name {
                        let kit = HandlerKit {
                            instance_info: instance_info.clone(),
                            serverless_storage: serverless_storage.clone(),
                        };
                        x = Some(Arc::new(#path::new(kit).await));
                    }
                };
            }
        }
        for handler in &deployment.handlers.clone().unwrap_or(vec![]) {
            // Add scaler check.
            let path: syn::Path = syn::parse_str(&handler.path).unwrap();
            let name = &handler.name;
            all_handler_checks = quote! {
                #all_handler_checks
                if x.is_none() && namespace == #namespace && name == #name {
                    let kit = HandlerKit {
                        instance_info: instance_info.clone(),
                        serverless_storage: serverless_storage.clone(),
                    };
                    x = Some(Arc::new(#path::new(kit).await));
                }
            };
        }
    }

    let result = quote! {
        static ref HANDLER_WRAPPER: async_once::AsyncOnce<Arc<ServerlessWrapper>> = async_once::AsyncOnce::new(async {
            let instance_info = Arc::new(InstanceInfo::new().await.unwrap());
            let serverless_storage = ServerlessStorage::new_from_info(instance_info.clone()).await.unwrap();
            let (namespace, name) = if let Some(service_name) = &instance_info.service_name {
                (&instance_info.subsystem, service_name)
            } else {
                (&instance_info.namespace, instance_info.handler_name.as_ref().unwrap())
            };
            let mut x: Option<Arc<dyn ServerlessHandler>> = None;
            #all_handler_checks
            Arc::new(ServerlessWrapper::new(instance_info, serverless_storage, x.unwrap()).await)
        });
    };

    result
}

pub fn gen_wrapper_main() -> TokenStream {
    quote! {
        if mode.contains("lambda") {
            let func = service_fn(lambda_wrapper);
            lambda_runtime::run(func).await?;
            return Ok(());
        }
        if mode.contains("ecs") {
            wrapper_main().await;
            return Ok(());
        }
    }
}

pub fn gen_wrapper_aux() -> TokenStream {
    quote! {
        async fn lambda_wrapper(event: LambdaEvent<Value>) -> Result<Value, Error> {
            let (event, _context) = event.into_parts();
            let wrapper = HANDLER_WRAPPER.get().await.clone();
            Ok(wrapper.handle_lambda_message(event).await)
        }

        async fn graceful_wait(duration_secs: u64) {
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
            tokio::time::sleep(std::time::Duration::from_secs(duration_secs)).await;
        }

        async fn wrapper_main() {
            let wrapper = HANDLER_WRAPPER.get().await.clone();
            let meta = warp::header::<String>("obelisk-meta");
            let invoke_path = warp::path!("invoke")
                .and(warp::post())
                .and(meta)
                .and(warp::body::content_length_limit(1024 * 1024 * 1024)) // 1GB.
                .and(warp::body::bytes())
                .and(with_wrapper(wrapper))
                .and_then(warp_handler);
            let hello_world = warp::path::end().map(|| "Hello, World at root!");
            let routes = invoke_path.or(hello_world);
            let port = common::get_port();
            println!("Listening on port {port}");
            let listen_info = ([0, 0, 0, 0], port);
            let (_, server) = warp::serve(routes).bind_with_graceful_shutdown(
                listen_info,
                graceful_wait(60),
            );
            server.await;
            println!("Done listening on port {port}");
        }

        fn with_wrapper(wrapper: Arc<ServerlessWrapper>) -> impl Filter<Extract = (Arc<ServerlessWrapper>,), Error = std::convert::Infallible> + Clone {
            warp::any().map(move || wrapper.clone())
        }

        pub async fn warp_handler(meta: String, payload: bytes::Bytes, wrapper: Arc<ServerlessWrapper>) -> Result<impl warp::Reply, Infallible> {
            let payload: Vec<u8> = payload.to_vec();
            let (resp_meta, resp_payload) = wrapper.handle_ecs_message(meta, payload).await;
            Ok(warp::http::Response::builder()
                .status(200)
                .header("obelisk-meta", &resp_meta)
                .body(resp_payload)
                .unwrap()
            )
        }

    }
}
