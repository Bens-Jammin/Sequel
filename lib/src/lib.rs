pub mod backend;
pub use backend::table::mainmem::table::{self, Table};
pub use backend::utils::files::{table_pages_dir, table_index_dir, user_directory};
pub use backend::access::data::value::{FieldValue, ColumnType, MAX_SIZE_OF_STRING};