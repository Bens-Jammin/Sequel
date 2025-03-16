use std::{collections::HashMap, fs};
use sequel::backend::access::{data::value::ColumnType, locations::{index_dir, page_dir, TABLE_DIRECTORY}};
use tempdir::TempDir;

#[cfg(test)]
mod tests {
    use sequel::backend::{
        access::data::value::FieldValue, table::mainmem::table::{init_table, insert_row}, 
        
    };

    use super::*;

    // Test the table initialization function
    #[test]
    fn test_init_table() {
        // Setup: Use a temporary directory for testing
        let temp_dir = TempDir::new("test_init_table").expect("Unable to create temporary directory");

        // Set the global table directory to the temporary one
        let original_table_dir = std::env::var("TABLE_DIRECTORY").unwrap_or_default();
        std::env::set_var("TABLE_DIRECTORY", temp_dir.path().to_str().unwrap());

        // Table name and column structure
        let table_name = "test_table".to_string();
        let mut columns: Vec<(String, (ColumnType, bool))> = Vec::new();
        columns.push(("id".to_string(), (ColumnType::NUMBER, true)));
        columns.push(("username".to_string(), (ColumnType::STRING, false)));

        // Call the function
        init_table(table_name.clone(), columns);

        // Assertions: Ensure that directories are created
        assert!(fs::metadata(TABLE_DIRECTORY).is_ok()); // Check if table directory exists
        assert!(fs::metadata(page_dir(&table_name)).is_ok()); // Check if page directory exists
        assert!(fs::metadata(index_dir(&table_name)).is_ok()); // Check if index directory exists

        // Clean up
        std::env::set_var("TABLE_DIRECTORY", original_table_dir);
    }

    // Test insert_row function
    #[test]
    fn test_insert_row() {
        // Setup: Use a temporary directory for testing
        let temp_dir = TempDir::new("test_insert_row").expect("Unable to create temporary directory");

        // Set the global table directory to the temporary one
        let original_table_dir = std::env::var("TABLE_DIRECTORY").unwrap_or_default();
        std::env::set_var("TABLE_DIRECTORY", temp_dir.path().to_str().unwrap());

        // Table and row data
        let table_name = "test_table".to_string();
        let mut columns: Vec<(String, (ColumnType, bool))> = Vec::new();
        columns.push(("id".to_string(), (ColumnType::NUMBER, true)));
        columns.push(("name".to_string(), (ColumnType::STRING, false)));

        init_table(table_name.clone(), columns);

        let row = vec![
            FieldValue::NUMBER(1),
            FieldValue::STRING("Alice".to_string()),
        ];

        insert_row(&table_name, row);
        // Verify the state of the table

        // Clean up
        std::env::set_var("TABLE_DIRECTORY", original_table_dir);
    }
}
