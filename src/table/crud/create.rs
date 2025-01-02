use std::{collections::HashMap, fs};

use crate::access::{data::value::ColumnType, locations::{index_dir, page_dir, TABLE_DIRECTORY}};



/*
    --- TODO ---
(1) use a system of tableIDs instead of identifying tables by their name
(2) do something with the columns when creating a table <i.e. a metadata file?>
*/

pub fn init_table(table_name: String, _columns: HashMap<String, ColumnType>) {
    
    // setup folders
    fs::create_dir( TABLE_DIRECTORY       ).expect("Unable to create table folder");
    fs::create_dir( page_dir(&table_name) ).expect("Unable to create page folder for table");
    fs::create_dir( index_dir(&table_name)).expect("Unable to create index folder for table");

}