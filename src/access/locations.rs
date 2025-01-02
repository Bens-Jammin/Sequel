/// #### C:/Programming/Rust/sequel
pub(crate) const TABLE_DIRECTORY: &str = "C:/Programming/Rust/sequel";


pub(crate) fn page_dir(table_name: &str) -> String { format!("{}/{}/page", TABLE_DIRECTORY, table_name) }
pub(crate) fn index_dir(table_name: &str) -> String { format!("{}/{}/index", TABLE_DIRECTORY, table_name) }