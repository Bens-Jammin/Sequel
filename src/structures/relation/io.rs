use std::{collections::{BTreeMap, HashMap}, fs::{self, File, OpenOptions}, io::{Read, Write}};

use rust_xlsxwriter::Workbook;

use crate::structures::{column::{self, parse_into_field_value, Column, DataType, FieldValue}, db_err::DBError};
use super::table::Table;



///  -----------
///    SAVING 
///  -----------
impl Table {
    pub fn save(&self, local_path: String) -> Result<(), DBError> {

        let file_path = format!("{}/{}",local_path, relation_file_name( &self.to_file_name() ));
        let encoded_data = bincode::serialize(&self);
        if encoded_data.is_err() { return Err(DBError::DataBaseFileFailure(file_path.to_owned())) }
        let encoded_data = encoded_data.unwrap();

        // open the file in a way that it appends data to the end of the file, not overriding the data 
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path);

        if file.is_err() { return Err(DBError::DataBaseFileFailure(file_path.to_owned())) }
        let mut file = file.unwrap();
        
        let r = file.write_all(&encoded_data);
        if r.is_err() { return Err(DBError::DataBaseFileFailure(file_path)) }
        
        Ok(())
    }
}
pub fn save_index(save_dir: &str, table_name: &str, column_name: &str, tree: BTreeMap<FieldValue, Vec<usize>>) {

    let file_path: String = format!("{}/{}",save_dir, index_file_name(table_name, column_name));

    let encoded_data = bincode::serialize(&tree).unwrap();
    let mut file = File::create(file_path).unwrap();
    file.write_all(&encoded_data).unwrap();
}




impl Table{
    #[allow(dead_code)]
    fn file_name_for_export(&self, file_extension: &str) -> String {
        format!("sequelDB_{}.{}", &self.name, file_extension)
    }


}


/// ---------------
///      IMPORT
/// ----------------
// TODO: implement importing CSV / XLSX
pub fn import_xlsx() {
    
}


pub fn import_csv(filepath: &str, delimeter: &str) -> Result<Table, DBError>  {

    let file_data = fs::read( filepath ).map_err(
        |_| DBError::IOFailure(filepath.to_string(), "unable to read data from file".to_string() )
    )?;

    
    let file_data_as_char = file_data
        .iter()
        .map(|v| *v as char)
        .collect::<String>()
    ;

    let idx_of_last_char_as_byte = file_data_as_char.len() - 1;

    let file_data_as_rows = file_data_as_char
        [0..idx_of_last_char_as_byte] // slice to remove linefeed char at the end
        .split("\n")
        .collect::<Vec<&str>>()
    ;
    
    let cells_of_data = file_data_as_rows
        .iter()
        .map(|s| 
            s
            .split(delimeter)
            .collect()
        )
        .collect::<Vec<Vec<&str>>>()
    ;

    let mut column_names: Vec<String> = Vec::new();
    let mut column_datatypes: Vec<DataType> = Vec::new();
    let mut columns: Vec<Column> = Vec::new();

    for column in &cells_of_data[0] {
        column_names.push( column.to_string() );
    }
    for datatype in &cells_of_data[1] {
        column_datatypes.push( column::parse_str(&datatype) );   
    }

    for (name, data_type) in column_names.iter().zip( column_datatypes ) {
        columns.push( column::Column::new(name.to_string(), data_type, true) );
    }
    let mut table = Table::new(
        "table from imported csv".to_string(),
        columns,
        true
    );

    for row_data in cells_of_data[2..].iter() {
        let mut row: HashMap<String, FieldValue> = HashMap::new();

        for (idx, col) in column_names.iter().enumerate() {
            let cell_value = parse_into_field_value( &row_data[idx].to_string() );
            row.insert( col.to_string(), cell_value );
        }
        
        table.insert_row(&row)?;
    }
    
    Ok(table)
}




/// ---------------
///     EXPORT
/// ---------------
impl Table {

    pub fn export_to_xlsx(&self, path: &str, row_offset: usize, col_offset: usize, min_col_width: f64) -> Result<(), DBError> {
        let file_path = format!("{}/{}", path, self.file_name_for_export("xlsx"));
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        
        // set column widths
        for (idx, col) in self.columns().iter().enumerate() {
            let mut max_cell_size = 0 as usize;
            for row in self.rows() {
                let cell_size = row
                    .get(col.get_name())
                    .unwrap()
                    .to_string()
                    .len();
                
                if max_cell_size < cell_size {
                    max_cell_size = cell_size;
                }
                
            }
    
            let col_width = if max_cell_size < (min_col_width as usize) { min_col_width } else { max_cell_size as f64 };
            worksheet.set_column_width( (row_offset+idx).try_into().unwrap() , col_width ).unwrap();
        }
    
        for (row_idx , row) in self.rows().iter().enumerate() {
            for (col_idx, col) in self.columns().iter().enumerate() {
                let cell = row.get(col.get_name()).unwrap();
    
                let xlxs_row_number: u32 = (row_offset + row_idx).try_into().unwrap();
                let xlxs_col_number: u16 = (col_offset + col_idx).try_into().unwrap();
    
                worksheet.write(xlxs_row_number, xlxs_col_number, format!( "{}", cell )).unwrap();
            }
        }
    
        workbook.save( file_path ).unwrap();
        Ok(())
    }


    pub fn export_to_csv(&self, path: &str, delimiter: &str ) -> Result<(), DBError> {

        let path = &format!("{}/{}", path,  &self.file_name_for_export("csv") );

        let number_of_cols = self.columns().len();
        
        // create the file if it doesn't exist
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open( path )
            .map_err(
                |_| DBError::IOFailure(path.to_owned(), "unable to open file".to_string())
            )?;


        let mut table_data_as_csv: String = String::new();
        for (idx, col) in self.columns().iter().enumerate() {
            table_data_as_csv.push_str( col.get_name() );            
            if idx + 1 != number_of_cols {
                table_data_as_csv.push_str( delimiter );
            } else {
                table_data_as_csv.push_str( "\n" );
            }
        }

        for (idx, col) in self.columns().iter().enumerate() {
            let col_data_type_as_str = format!("{}", col.get_data_type() ); 
            table_data_as_csv.push_str( col_data_type_as_str.as_str() );
        
            if idx + 1 != number_of_cols {
                table_data_as_csv.push_str( delimiter );
            } else {
                table_data_as_csv.push_str( "\n" );
            }
        }




        for row in self.rows() {
            let mut formatted_row_data: String = String::new();

            for (idx, col) in self.columns().iter().enumerate() {
                let data = row[col.get_name()].to_string();
                
                formatted_row_data.push_str( &data );
                // last item, no need to add delimiter
                if idx + 1 != number_of_cols {
                    formatted_row_data.push_str( delimiter )
                } else {
                    formatted_row_data.push_str( "\n" );
                }
            }
            table_data_as_csv.push_str( &formatted_row_data );
        }


        file.write_all( table_data_as_csv.as_bytes() ).map_err(
            |_| return  DBError::IOFailure(path.to_owned(), "Failed to write data to CSV".to_owned())
        )?;

        Ok(())
    } 

    
}

/// -------------
///     LOAD
/// -------------


/// loads a database given a filepath. File must be a binary file (extension .bin)
/// 
/// ### Note
/// as of October 2024, the database files are saved in the form "db_{database name}.bin",
/// where the database name is capitalized, and spaces are replaced with underscores
/// 
/// ### Examples
/// Valid files:
/// - db_EMPLOYEES.bin
/// - db_WAGES_2024.bin
/// 
/// Invalid files:
/// - db_Employees.bin
/// - wages_2024.bin
/// - db_election_results.csv
pub fn load_database(file_path: &str) -> Result<Table, DBError> {
    
    let file = File::open(file_path);
    if file.is_err() { return Err(DBError::DataBaseFileFailure(file_path.to_owned()))}
    let mut file = file.unwrap();
    
    let mut buffer = Vec::new();
    let r = file.read_to_end(&mut buffer);
    if r.is_err() { return Err(DBError::DataBaseFileFailure(file_path.to_owned())) }
    
    
    let decoded_data = bincode::deserialize(&buffer);
    
    if decoded_data.is_err() { 
        return Err(DBError::DataBaseFileFailure(file_path.to_owned()))
    } else {
        Ok(decoded_data.unwrap())
    }
}



pub fn load_index(save_dir: &str, table_name: &str, column_name: &str) -> Option<BTreeMap<FieldValue, Vec<usize>>> {
    let file_path: String = format!("{}/{}", save_dir, index_file_name(table_name, column_name));
    let file = File::open(file_path);
    if file.is_err() { return None; }
    let mut file = file.unwrap(); 
    
    let mut data_buffer = Vec::new();
    let r = file.read_to_end(&mut data_buffer);
    if r.is_err() { return None; }
    
    
    let tree = bincode::deserialize(&data_buffer);
    if tree.is_err() { return None; }
    
    Some(tree.unwrap())    
}



/// ---------------
///      MISC
/// ---------------


pub fn index_file_name(table_name: &str, column_name: &str) -> String {
    format!("idx_{}_{}.bin", table_name, column_name)
}


pub fn relation_file_name(name: &String) -> String {
    format!("db_{}.bin", format_for_file_name(name) )
}


/// converts a string into its file name counterpart. Used to help find a file for a possible relation
pub fn format_for_file_name(str: &str) -> String {
    str.to_uppercase().replace(" ", "_")
}

impl Table {
    
    /// gives the formatted name to be used as a file name
    /// 
    /// ## Example
    /// let table = Table::new(...);
    /// table.name() -> "Example Table Name"
    /// table.to_file_name() -> "EXAMPLE_FILE_NAME"
    pub fn to_file_name(&self) -> String {
        let name = &self.name;
        name.to_uppercase().replace(" ", "_")
    }
    
}