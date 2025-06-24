use std::fs::File;
use std::io::Write;
use crate::{
    backend::{
        access::catalog::syscat::read_syscat, 
        table::mainmem::table::TableIterator, utils::files::table_directory
    }, 
     
    Table
};


const ASCII_TABLE_FORMAT: &str = "     ══            "; // header sep only 

/// disable column types removed the <TYPE> for each header of the table 
pub fn view(username: &str, table_name: &str, disable_column_types: bool, minimum: usize, maximum: usize) {

    let path_to_table = table_directory( username, table_name );
    let syscat = read_syscat(&path_to_table).unwrap();
    let mut text_table = comfy_table::Table::new();
    let mut header_row: Vec<comfy_table::Cell> = Vec::new();
    
    for col in &syscat.columns {
        let cell = if disable_column_types {
            comfy_table::Cell::new(format!("{}", &col.name ))
        } else {
            comfy_table::Cell::new(format!("{}\n<{}>", &col.name, &col.data_type ))
        }
        .set_alignment(comfy_table::CellAlignment::Center);
        header_row.push(cell); 
    }
    text_table.set_header(header_row);

    let iterator_name = String::from(table_name);
    let usr = username.to_string();
    let table_iterator = TableIterator::init( &iterator_name, &usr );
    
    for (idx,record) in table_iterator.enumerate() {
        if idx < minimum || idx > maximum { continue; }  
        let data = record.data_as_mut();
        let formatted_row: Vec<String> = data
            .iter()
            .map(|v| return v.to_string())
            .collect::<Vec<String>>();
        text_table.add_row(formatted_row);

    } 


    text_table.load_preset(ASCII_TABLE_FORMAT);
    
    println!("\n{}", text_table.to_string())
}



impl Table {
    pub fn as_string(&self, minimum: usize, maximum: usize) -> String {
        let syscat = &self.syscat;
        let mut text_table = comfy_table::Table::new();
        let mut header_row: Vec<comfy_table::Cell> = Vec::new();
        
        for col in &syscat.columns {
            let cell = comfy_table::Cell::new(format!("{}", &col.name ))
                .set_alignment(comfy_table::CellAlignment::Center);
            header_row.push(cell);
        }
        text_table.set_header(header_row);

        let iterator_name = String::from(&self.name);
        let table_iterator = TableIterator::init( &iterator_name, &self.user );
        
        for (idx, record) in table_iterator.enumerate() {
            
            // window filter
            if idx < minimum || idx > maximum { continue; }  

            let data = record.data_as_mut();
            let formatted_row: Vec<String> = data
                .iter()
                .map(|v| return v.to_string())
                .collect::<Vec<String>>();
            text_table.add_row(formatted_row);
        }     
        text_table.load_preset(ASCII_TABLE_FORMAT);

        text_table.to_string()
    }
}