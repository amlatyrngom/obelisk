use super::Deployment;
use proc_macro2::TokenStream;
use quote::quote;

pub fn gen_messaging_static_code(deployments: &[Deployment]) -> TokenStream {
    let mut all_messaging_checks = quote! {};
    // Add services and rescalers.
    for deployment in deployments.iter() {
        if let Some(messaging) = &deployment.messaging {
            let namespace = &deployment.namespace;
            // Add scaler check.
            let path: syn::Path = syn::parse_str(&messaging.path).unwrap();
            all_messaging_checks = quote! {
                #all_messaging_checks
                if x.is_none() && namespace == #namespace {
                    x = Some(Arc::new(#path::new(&name, plog.clone()).await));
                }
            };
        }
    }

    let result = quote! {
        static ref MESSAGING_FUNCTION: async_once::AsyncOnce<Arc<dyn FunctionInstance>> = async_once::AsyncOnce::new(async {
            // This will fail if lock is ever lost.
            let namespace = std::env::var("NAMESPACE").unwrap();
            let name = std::env::var("NAME").unwrap();
            let plog = Arc::new(persistence::PersistentLog::new(&namespace, &name).await);
            let mut x: Option<Arc<dyn ActorInstance>> = None;
            #all_messaging_checks
            let handler: Arc<dyn FunctionInstance> = Arc::new(MessagingHandler::new(plog.clone(), x.unwrap(), &namespace, &name).await);
            handler
        });

        static ref MESSAGING_BACKEND: async_once::AsyncOnce<Arc<AdapterBackend>> = async_once::AsyncOnce::new(async {
            // This will fail if lock is ever lost.
            let svc_info = Arc::new(ServiceInfo::new().await.unwrap());
            let namespace = svc_info.namespace.clone();
            let name = svc_info.name.clone();
            let plog = Arc::new(persistence::PersistentLog::new(&namespace, &name).await);
            let mut x: Option<Arc<dyn ActorInstance>> = None;
            #all_messaging_checks
            let handler = Arc::new(MessagingHandler::new(plog.clone(), x.unwrap(), &namespace, &name).await);
            Arc::new(AdapterBackend::new(svc_info, handler).await)
        });
    };

    result
}

pub fn gen_messaging_main() -> TokenStream {
    let result = quote! {
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
    };

    result
}

pub fn gen_messaging_aux() -> TokenStream {
    let result = quote! {
        async fn messaging_lambda_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
            let (event, _context) = event.into_parts();
            let function = MESSAGING_FUNCTION.get().await.clone();
            Ok(function.invoke(event).await)
        }
    };

    result
}
