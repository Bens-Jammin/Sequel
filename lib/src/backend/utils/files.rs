use std::fs;
use std::path::Path;
use std::io;

pub fn clear_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    if path.as_ref().is_dir() {
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                fs::remove_dir_all(&entry_path)?;
            } else {
                fs::remove_file(&entry_path)?;
            }
        }
    }
    println!("directory cleared.");
    Ok(())
}


pub fn pages_directory(root: &str ) -> String { format!("{}/pages", root) }
pub fn index_directory(root: &str ) -> String { format!("{}/index", root) }
/// .../appdata/sequel/users/<db_username>
pub fn user_directory(username: &str ) -> String {     
    format!("{}/users/{}", database_dir(), &username)
}
pub fn table_directory(username: &str, table_name: &str) -> String {
    format!("{}/users/{}/{}",database_dir(), username, table_name)
}

pub fn table_pages_dir(table_name: &str) -> String { pages_directory(&user_directory(table_name)) }
pub fn table_index_dir(table_name: &str) -> String { index_directory(&user_directory(table_name)) }
// users/appdata/sequel
pub fn database_dir() -> String { format!("{}/Sequel", std::env::var("APPDATA").unwrap()) }