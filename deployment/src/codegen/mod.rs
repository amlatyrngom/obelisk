// mod functional;
// mod generic;
// mod messaging;
mod rescaling;
mod wrapper;

use super::{exec_cmd, BUILD_DIR};
use super::{Binary, CargoConfig, Deployment1};
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
    // let messaging_path = make_system_path("messaging", for_system);
    // let persistence_path = make_system_path("persistence", for_system);
    let imports = quote! {
        use lambda_runtime::{service_fn, LambdaEvent, Error};
        use serde_json::Value;
        use #common_path;
        // Used by rescaler.
        use #common_path::{ScalingStateRescaler, Rescaler, RescalerSpec};
        // Used by handlers.
        use #common_path::{ServerlessHandler, ServerlessWrapper, InstanceInfo, HandlerKit, FileLease};
        use lazy_static::lazy_static;
        use std::sync::Arc;
        use warp::Filter;
        use std::convert::Infallible;
        use tokio::signal::unix;
        use jemallocator::Jemalloc;
    };

    imports
}

fn gen_main_file_content(deployments: &[Deployment1], for_system: bool) -> String {
    let imports = gen_imports(for_system);
    let static_code = {
        let rescaling_code = rescaling::gen_rescaler_static_code(deployments);
        let wrapper_code = wrapper::gen_wrapper_static_code(deployments);
        quote! {
            lazy_static! {
                #rescaling_code
                #wrapper_code
            }
        }
    };
    let main_code = {
        let rescaling_code = rescaling::gen_rescaler_main();
        let wrapper_code = wrapper::gen_wrapper_main();
        quote! {
            #[global_allocator]
            static GLOBAL: Jemalloc = Jemalloc;

            #[tokio::main]
            async fn main() -> Result<(), Error> {
                println!("Main Started!");
                let mode = std::env::var("OBK_EXECUTION_MODE").unwrap();
                println!("Running mode: {mode}");
                #rescaling_code
                #wrapper_code
                panic!("Unknown mode!");
            }
        }
    };

    let aux_code = {
        let rescaling_code = rescaling::gen_rescaler_aux();
        let wrapper_code = wrapper::gen_wrapper_aux();
        quote! {
            #rescaling_code
            #wrapper_code
        }
    };

    let result = quote! {
        #imports
        #static_code
        #main_code
        #aux_code
    };
    result.to_string()
}

pub(crate) fn gen_main(deployments: &[Deployment1], for_system: bool) {
    let file_content = gen_main_file_content(deployments, for_system);
    // Write out
    let dir_path = std::path::Path::new(BUILD_DIR);
    if !dir_path.exists() {
        std::fs::create_dir_all(dir_path).unwrap();
    }
    let file_path = format!("{BUILD_DIR}/main.rs");
    std::fs::write(&file_path, file_content).unwrap();
    exec_cmd("cargo", vec!["fmt", "--", &file_path]);
}

pub(crate) fn gen_cargo(mut cargo_config: CargoConfig) {
    cargo_config.bin = Some(vec![Binary {
        name: "obelisk_main".into(),
        path: "src/main.rs".into(),
    }]);
    let deps = cargo_config.dependencies.as_table_mut().unwrap();

    if !deps.contains_key("lazy_static") {
        deps.insert("lazy_static".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("async_once") {
        deps.insert("async_once".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("lambda_runtime") {
        deps.insert("lambda_runtime".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("warp") {
        deps.insert("warp".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("bytes") {
        deps.insert("bytes".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("serde_json") {
        deps.insert("serde_json".into(), toml::Value::String("*".into()));
    }
    if !deps.contains_key("tokio") {
        deps.insert(
            "tokio".into(),
            toml::from_str(
                r#"
                version = "*"
                features = ["full"]
            "#,
            )
            .unwrap(),
        );
    }
    if !deps.contains_key("jemallocator") {
        deps.insert("jemallocator".into(), toml::Value::String("*".into()));
    }

    // Write out
    let cargo_config = toml::to_string_pretty(&cargo_config).unwrap();
    let dir_path = std::path::Path::new(BUILD_DIR);
    if !dir_path.exists() {
        std::fs::create_dir_all(dir_path).unwrap();
    }
    let file_path = format!("{BUILD_DIR}/Cargo.toml");
    std::fs::write(file_path, cargo_config).unwrap();
}
