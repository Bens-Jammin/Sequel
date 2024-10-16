/// This file is used to grab config files from the other projects which use this database
use std::{env, fs};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    // find user/appdata
    let appdata_dir = dirs::data_local_dir().expect("Could not find AppData directory");

    // Define directories for relations and indexes
    let relation_dir: PathBuf = appdata_dir.join("Sequel").join("Database").join("Relations");
    let index_dir: PathBuf = appdata_dir.join("Sequel").join("Database").join("Indexes");
    let export_dir: PathBuf = appdata_dir.join("Sequel").join("Database").join("Export");

    // Create the directories (and any necessary parent directories)
    if let Err(e) = fs::create_dir_all(&relation_dir) {
        panic!("Failed to create relations directory: {:?}", e);
    }
    if let Err(e) = fs::create_dir_all(&index_dir) {
        panic!("Failed to create indexes directory: {:?}", e);
    }
    if let Err(e) = fs::create_dir_all(&export_dir) {
        panic!("Failed to create export directory: {:?}", e);
    }

    // Ensure build.rs is re-run if it changes
    println!("cargo:rerun-if-changed=build.rs");

    // Access environment variables or fallback to defaults
    let relation_path = env::var("RELATION_PATH").unwrap_or_else(|_| relation_dir.to_string_lossy().to_string());
    let index_path = env::var("INDEX_PATH").unwrap_or_else(|_| index_dir.to_string_lossy().to_string());
    let export_path = env::var("EXPORT_PATH").unwrap_or_else(|_| export_dir.to_string_lossy().to_string());

    // Create a config file with the generated paths
    let mut file = File::create("src/config.rs").unwrap();
    write!(
        file,
        r#"
        pub const RELATION_PATH: &str = r"{}";
        pub const INDEX_PATH: &str = r"{}";
        pub const EXPORT_PATH: &str = r"{}";
        "#,
        relation_path,
        index_path,
        export_path
    )
    .unwrap();
}
