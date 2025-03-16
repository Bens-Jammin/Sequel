use sequel::backend::{
    access::data::{page::Page, value::{ColumnType, FieldValue}}, 
    display::cli::view, table::mainmem::table::{filter_table, init_table, insert_row, Condition}, 
};




pub fn main() {

    
    let table_name = "Sample Table".to_string();
    let mut cols: Vec<(String, (ColumnType, bool))> = Vec::new();
    cols.push((
        "column A".to_string(),
        (ColumnType::NUMBER, true)
    ));
    cols.push((
        "column B".to_string(),
        (ColumnType::STRING, false)
    ));
    cols.push((
        "column C".to_string(),
        (ColumnType::BOOLEAN, false)
    ));
    
    init_table( "Sample Table".to_string(), cols ); 
    
    insert_row( &table_name, vec![
        FieldValue::NUMBER(1), 
        FieldValue::STRING("Hello, World!".to_string()),
        FieldValue::BOOL(false)
    ]);
    insert_row( &table_name, vec![
        FieldValue::NUMBER(2), 
        FieldValue::STRING("Look at my table!".to_string()),
        FieldValue::BOOL(false)
    ]);
    insert_row(&table_name, vec![
        FieldValue::NUMBER(3), 
        FieldValue::STRING("I upgraded my code".to_string()),
        FieldValue::BOOL(false)
    ]);
    insert_row(&table_name, vec![
        FieldValue::NUMBER(4), 
        FieldValue::STRING("So hopefully it runs".to_string()),
        FieldValue::BOOL(true)
    ]);
    insert_row(&table_name, vec![
        FieldValue::NUMBER(5), 
        FieldValue::STRING("faster and smaller than version one".to_string()),
        FieldValue::BOOL(true)
    ]);
    let p = Page::read_page(1, "Sample Table").unwrap();
    let fp = Page::read_page(1, "filtered Sample Table").unwrap();
    
    // view(&table_name);

    filter_table( String::from(table_name), "column A", Condition::GreaterThan(FieldValue::NUMBER(3)) );
    
    view("filtered Sample Table");
}