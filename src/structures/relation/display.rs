use comfy_table::presets::ASCII_MARKDOWN;

use super::table::Table;



impl Table {
    pub fn to_ascii(&self) -> String {

        let mut text_table = comfy_table::Table::new();

        let mut header_row: Vec<comfy_table::Cell> = Vec::new();
        for col in self.columns() {
            let cell = comfy_table::Cell::new(format!("{}\n<{}>", col.get_name(), col.get_data_type() ))
            .set_alignment(comfy_table::CellAlignment::Center);
            header_row.push(cell);

        }

        text_table.set_header(header_row);

        for row in self.rows() {
            let mut formatted_row: Vec<String> = Vec::new();
            for col in self.columns() {
                formatted_row.push( row.get(col.get_name()).unwrap().to_string() )
            }
            text_table.add_row(formatted_row);
        }

        text_table.load_preset(ASCII_MARKDOWN).remove_style(comfy_table::TableComponent::HorizontalLines);
        
        format!("\n{}", text_table.to_string())
    }
} 