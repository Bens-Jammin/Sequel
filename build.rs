/// This file is used to grab config files from the other projects which use this database
use std::env;
use std::fs::File;
use std::io::Write;

fn main() {
    // Access environment variables
    let relation_path = env::var("RELATION_PATH").unwrap_or("C:/Sequel/Database/Relations".to_string());
    let index_path = env::var("INDEX_PATH").unwrap_or("C:/Sequel/Database/Index".to_string());
    
    // Create a config file or generate code with the config values
    let mut file = File::create("src/config.rs").unwrap();
    write!(file, r#"
        pub const RELATION_PATH: &str = "{}";
        pub const INDEX_PATH: &str = "{}";
    "#, relation_path, index_path).unwrap();
}
