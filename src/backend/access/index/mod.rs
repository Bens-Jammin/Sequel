pub(crate) mod bitmap;
pub(crate) mod bplus_tree;
pub(crate) mod hash; 


trait Index {
    fn create_index(table_name: &str, column_index: usize);
    fn load_index(table_name: &str, column_index: usize, bitmap_type_name: &str ) -> Self where Self: Sized;
    fn save_index(&self, table_name: &str, column_index: usize, col_type_name: &str );
}


pub(super) fn index_file_name(table_name: &str, column_index: usize, index_name: &str,) -> String {
    format!("sequelINDEX_{}_{}_{}.idx", index_name, table_name, column_index )
}