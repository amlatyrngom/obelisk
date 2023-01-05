mod functional;
mod generic;
mod messaging;
mod rescaling;

use super::{exec_cmd, BUILD_DIR};
use super::{Binary, CargoConfig, Deployment};
use quote::quote;

fn gen_main_file_content(deployments: &[Deployment], for_system: bool) -> String {
    let imports = generic::gen_imports(for_system);
    let static_code = {
        let generic_code = generic::gen_generic_static_code(deployments);
        let rescaling_code = rescaling::gen_rescaler_static_code(deployments);
        let functional_code = functional::gen_functional_static_code(deployments);
        let messaging_code = messaging::gen_messaging_static_code(deployments);
        quote! {
            lazy_static! {
                #generic_code
                #rescaling_code
                #functional_code
                #messaging_code
            }
        }
    };
    let main_code = {
        let generic_code = generic::gen_generic_main();
        let rescaling_code = rescaling::gen_rescaler_main();
        let functional_code = functional::gen_functional_main(for_system);
        let messaging_code = messaging::gen_messaging_main(for_system);
        quote! {
            #[tokio::main]
            async fn main() -> Result<(), Error> {
                println!("Main Started!");
                let mode = std::env::var("EXECUTION_MODE").unwrap();
                println!("Running mode: {mode}");
                #generic_code
                #rescaling_code
                #functional_code
                #messaging_code
                panic!("Unknown mode!");
            }
        }
    };

    let aux_code = {
        let generic_code = generic::gen_generic_aux();
        let rescaling_code = rescaling::gen_rescaler_aux();
        let functional_code = functional::gen_functional_aux();
        let messaging_code = messaging::gen_messaging_aux(for_system);
        quote! {
            #generic_code
            #rescaling_code
            #functional_code
            #messaging_code
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

pub(crate) fn gen_main(deployments: &[Deployment], for_system: bool) {
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
            toml::toml!(
                version = "*"
                features = ["full"]
            ),
        );
    }

    // Write out
    let cargo_config = toml::to_string_pretty(&cargo_config).unwrap();
    let dir_path = std::path::Path::new(BUILD_DIR);
    if !dir_path.exists() {
        std::fs::create_dir_all(dir_path).unwrap();
    }
    let file_path = format!("{BUILD_DIR}/Cargo.toml");
    std::fs::write(&file_path, cargo_config).unwrap();
}
