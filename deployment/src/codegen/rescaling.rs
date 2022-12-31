use super::Deployment;
use proc_macro2::TokenStream;
use quote::quote;

pub fn gen_rescaler_static_code(deployments: &[Deployment]) -> TokenStream {
    let mut all_rescalers_checks = quote! {};
    // Add services and rescalers.
    for deployment in deployments.iter() {
        if let Some(subsystem) = &deployment.subsystem {
            let subsystem_name = &deployment.namespace;
            // Add scaler check.
            let path: syn::Path = syn::parse_str(&subsystem.rescaler_path).unwrap();
            all_rescalers_checks = quote! {
                #all_rescalers_checks
                if x.is_none() && subsystem == #subsystem_name {
                    x = Some(Arc::new(#path::new().await));
                }
            };
        }
    }

    let result = quote! {
        static ref RESCALER: async_once::AsyncOnce<Arc<AdapterScaling>> = async_once::AsyncOnce::new(async {
            let subsystem = std::env::var("SUBSYSTEM").unwrap();
            let mut x: Option<Arc<dyn Rescaler>> = None;
            #all_rescalers_checks
            Arc::new(AdapterScaling::new(x, &subsystem).await)
        });
    };

    result
}

pub fn gen_rescaler_main() -> TokenStream {
    quote! {
        if mode == "rescaler" {
            let func = service_fn(rescaler_handler);
            lambda_runtime::run(func).await?;
            return Ok(());
        }
    }
}

pub fn gen_rescaler_aux() -> TokenStream {
    quote! {
        async fn rescaler_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
            let (event, _context) = event.into_parts();
            let rescaler = RESCALER.get().await.clone();
            Ok(rescaler.invoke(event).await)
        }
    }
}
