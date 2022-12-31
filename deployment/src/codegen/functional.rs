use super::Deployment;
use proc_macro2::TokenStream;
use quote::quote;

pub fn gen_functional_static_code(deployments: &[Deployment]) -> TokenStream {
    let mut all_functional_checks = quote! {};
    for deployment in deployments.iter() {
        if let Some(functional) = &deployment.functional {
            let path: syn::Path = syn::parse_str(&functional.path).unwrap();
            let functional_namespace = &deployment.namespace;
            all_functional_checks = quote! {
                #all_functional_checks
                if x.is_none() && namespace == #functional_namespace {
                    x = Some(Arc::new(#path::new().await));
                }
            };
        }
    }

    let mut result = quote! {
        static ref FUNCTION_INSTANCE: async_once::AsyncOnce<Arc<dyn FunctionInstance>> = async_once::AsyncOnce::new(async {
            let namespace = std::env::var("NAMESPACE").unwrap();
            let mut x: Option<Arc<dyn FunctionInstance>> = None;
            #all_functional_checks
            x.unwrap()
        });
    };

    for deployment in deployments.iter() {
        if deployment.namespace == "functional" {
            let invoker_path = deployment.subsystem.clone().unwrap().service_path;
            let invoker_path: syn::Path = syn::parse_str(&invoker_path).unwrap();
            result = quote! {
                #result
                static ref FUNCTIONAL_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> = async_once::AsyncOnce::new(async {
                    let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
                    let invoker = Arc::new(#invoker_path::new(svc_info.clone()).await);
                    Arc::new(AdapterBackend::new(svc_info, invoker).await)
                });
            }
        }
    }

    return result;
}

pub fn gen_functional_main(for_system: bool) -> TokenStream {
    let mut result = quote! {
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
    };

    if for_system {
        result = quote! {
            #result
            if mode == "functional_backend" {
                let backend = FUNCTIONAL_BACKEND.get().await.clone();
                generic_backend_main(backend).await;
                return Ok(());
            }
        };
    }

    result
}

pub fn gen_functional_aux() -> TokenStream {
    let function_event_handler = quote! {
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
    };

    let result = quote! {
        async fn functional_http_handler(function_instance: Arc<dyn FunctionInstance>) {
            let timeout = std::env::var("TIMEOUT").unwrap_or("5".into());
            let timeout: u64 = timeout.parse().unwrap();
            let invoke_path = warp::path!("invoke")
                .and(warp::post())
                // Only accept bodies smaller than 8MB...
                .and(warp::body::content_length_limit(1024 * 1024 * 8))
                .and(warp::body::json())
                .and(with_function_instance(function_instance))
                .and_then(warp_functional_handler);
            let hello_world = warp::path::end().map(|| "Hello, World at root!");
            let routes = invoke_path.or(hello_world);
            let port = common::get_port().unwrap();
            println!("Listening on port {port}");
            let listen_info = ([0, 0, 0, 0], port);
            let (_, server) = warp::serve(routes).bind_with_graceful_shutdown(
                listen_info,
                graceful_wait(timeout),
            );
            server.await;
            println!("Done listening on port {port}");
        }

        fn with_function_instance(function: Arc<dyn FunctionInstance>) -> impl Filter<Extract = (Arc<dyn FunctionInstance>,), Error = std::convert::Infallible> + Clone {
            warp::any().map(move || function.clone())
        }

        async fn functional_lambda_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
            let (event, _context) = event.into_parts();
            let function = FUNCTION_INSTANCE.get().await.clone();
            #function_event_handler
            Ok(serde_json::to_value(&resp).unwrap())
        }

        pub async fn warp_functional_handler(event: Value, function: Arc<dyn FunctionInstance>) -> Result<impl warp::Reply, Infallible> {
            #function_event_handler
            Ok(warp::reply::json(&resp))
        }
    };

    result
}
