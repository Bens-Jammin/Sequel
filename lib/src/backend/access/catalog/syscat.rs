/* stores: (dashed boxes mean not implemented)


[X] 1. Table name
[X] 2. Table ID
[X] 3. column details
    [X] a. type of data
    [-] b. range of data (probably won't be used, all sizes are static)
    [X] c. required (allows NULLs or not)
    [-] d. key
        [-] i. if foreign, which table it relates to
[X] 3. index details
    [X] a. type
    [X] b. columns indexed
[-] 4. storage data
    [-] a. page ids
    [-] b. number of rows

*/

use std::{
    fs::{self, File}, 
    io::{self, Read},
    sync::atomic::{AtomicU16, Ordering}
};
use serde::{Serialize, Deserialize};
use crate::backend::access::data::value::ColumnType;



static TABLE_ID_COUNTER: AtomicU16 = AtomicU16::new(1);
fn next_table_id() -> u16 { TABLE_ID_COUNTER.fetch_add(1, Ordering::Relaxed) }


#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnMetaData {
    pub name: String,
    pub data_type: ColumnType,
    pub allows_nulls: bool
}


#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMetaData {
    pub index_type: String, // TODO: update to be an enum
    pub column_indexing: String
}


#[derive(Serialize, Deserialize, Debug)]
pub struct SystemCatalog {
    pub username: String,
    pub data_dir: String,
    pub table_name: String,
    pub table_id: u16,
    pub total_pages: u16,
    pub free_pages: Vec<u8>,
    pub next_record_id: u32,
    pub columns: Vec<ColumnMetaData>,
    pub indices: Vec<IndexMetaData>,
    pub next_page_id: u8
}




/// iniitalizes a system catalog. Each table has its own unique system catalog
/// Using a nested tuple instead of a hashmap to maintain order in the columns
/// TODO: use a hash map eventually but maintaining column order?
pub fn init_syscat(username: &str, table_name: &str, column_data: &Vec<(String, (ColumnType, bool))>, data_dir: String ) {

    let file_path = format!( "{}/syscat.txt", &data_dir );
    let file = File::create( file_path ).unwrap();

    let indexes: Vec<IndexMetaData> = collect_index_data( &table_name, &data_dir ).expect("failed to load data about table indices.");
    
    let syscat = SystemCatalog {
        username: username.to_string(), 
        data_dir,
        table_name: table_name.to_string(),
        table_id: next_table_id(),
        next_record_id: 1,
        next_page_id: 1,
        free_pages: Vec::new(),
        columns: clean_column_data( column_data),
        indices: indexes,
        total_pages: 0
    };
    
    serde_json::to_writer( file, &syscat ).unwrap();

} 



fn clean_column_data(col_data: &Vec<(String, (ColumnType, bool))> ) -> Vec<ColumnMetaData> {

    let mut cleaned_data: Vec<ColumnMetaData> = Vec::new();

    for (name, (data_type, allows_nulls)) in col_data.iter() {
        cleaned_data.push( ColumnMetaData {
            name: name.to_string(),
            data_type: data_type.clone(),
            allows_nulls: *allows_nulls,
        });
    }

    cleaned_data
}



/// collects the index type, and the name of the column which it indexes for all indices in the table.
/// 
/// #### NOTE: 
/// This function was made entirely with chatGPT, so it probably sucks ass 
/// (i am currently sick and don't give a shit, i just wanted it done)
/// 
/// # WARNING:
/// currently does nothing: this just returns an empty string
fn collect_index_data(_table_name: &str, _data_dir: &str) -> io::Result<Vec<IndexMetaData>> {

    let index_data = Vec::new();

    // println!("[syscat/collect index data]: directory: {data_dir}");
    // // Replace "index_dir" with the actual directory path
    // let path = index_directory( table_name );
    // let index_dir = Path::new( &path );

    // for entry in fs::read_dir(index_dir)? {
    //     let entry = entry?;
    //     let path = entry.path();

    //     if let Some(file_name) = path.file_name().and_then(|f| f.to_str()) {

    //         let parts: Vec<&str> = file_name.split('_').collect();
    //         if parts.len() > 3 {
    //             index_data.push( IndexMetaData {
    //                 index_type: parts[1].to_string(),
    //                 column_indexing: parts[2].to_string(),
    //             })
    //         }
    //     }
    // }

    Ok(index_data)
}





pub fn update_syscat_on_disk(system_catalog: &SystemCatalog) {

    let file_path = format!( "{}/syscat.txt", &system_catalog.data_dir );
    let file = File::create( file_path ).unwrap();

    serde_json::to_writer(file, system_catalog).unwrap();
}


pub fn read_syscat(data_dir: &str) -> Result<SystemCatalog, String> { 

    let syscat_path = format!("{}/syscat.txt", data_dir);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open( syscat_path )
        .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let catalog: SystemCatalog = serde_json::from_str(&contents).unwrap();
    Ok(catalog)
}