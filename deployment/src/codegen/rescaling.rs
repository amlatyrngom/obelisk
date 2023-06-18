use super::Deployment1;
use proc_macro2::TokenStream;
use quote::quote;

pub fn gen_rescaler_static_code(deployments: &[Deployment1]) -> TokenStream {
    let mut all_rescalers_checks = quote! {};
    // Add services and rescalers.
    for deployment in deployments.iter() {
        if let Some(subsystem) = &deployment.subsystem {
            let namespace = &deployment.namespace;
            // Add scaler check.
            let path: syn::Path = syn::parse_str(&subsystem.rescaler.path).unwrap();
            all_rescalers_checks = quote! {
                #all_rescalers_checks
                if x.is_none() && namespace == #namespace {
                    x = Some(Arc::new(#path::new().await));
                }
            };
        }
    }

    let result = quote! {
        static ref RESCALER: async_once::AsyncOnce<Arc<ScalingStateRescaler>> = async_once::AsyncOnce::new(async {
            let spec = std::env::var("OBK_RESCALER_SPEC").unwrap();
            let spec: RescalerSpec = serde_json::from_str(&spec).unwrap();
            let namespace = spec.namespace;
            let mut x: Option<Arc<dyn Rescaler>> = None;
            #all_rescalers_checks
            Arc::new(ScalingStateRescaler::new(&namespace, x.unwrap()).await)
        });
    };

    result
}

pub fn gen_rescaler_main() -> TokenStream {
    quote! {
        if mode == "rescaler_lambda" {
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
            let resp = rescaler.handle_lambda_request(event.clone()).await;
            match resp {
                Ok(resp) => {
                    println!("Rescaling ({event:?}) Results: {resp:?}");
                    Ok(resp)
                },
                Err(e) => {
                    panic!("Rescaling Erro");
                }
            }
        }
    }
}
