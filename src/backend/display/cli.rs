use comfy_table::presets::ASCII_MARKDOWN;

use crate::backend::{access::catalog::syscat::read_syscat, table::mainmem::table::TableIterator};




pub fn view(table_name: &str) {

    let syscat = read_syscat(table_name).unwrap();

    let mut text_table = comfy_table::Table::new();

    let mut header_row: Vec<comfy_table::Cell> = Vec::new();
    for col in &syscat.columns {
        let cell = comfy_table::Cell::new(format!("{}\n<{}>", &col.name, &col.data_type ))
        .set_alignment(comfy_table::CellAlignment::Center);
        header_row.push(cell);

    }

    text_table.set_header(header_row);

    let iterator_name = String::from(table_name);
    let table_iterator = TableIterator::init( &iterator_name );

    for record in table_iterator {
        let data = record.data_as_mut();
        let formatted_row: Vec<String> = data
            .iter()
            .map(|v| return format!("{v}"))
            .collect::<Vec<String>>();
    
        text_table.add_row(formatted_row);

    } 


    text_table.load_preset(ASCII_MARKDOWN).remove_style(comfy_table::TableComponent::HorizontalLines);
    
    println!("\n{}", text_table.to_string())
}