

/*
    --- TODO ---
(1) Change the logic in the `load()` and `save()` methods
    to use custom serialization / deserialization
    
*/

use std::{fs::{File, OpenOptions}, io::{Read, Write}, path::Path};

use crate::backend::access::{data::{page::Page, value::FieldValue}, index::index_file_name, locations::index_dir};

use super::Index;


/// ### NOTE: Bitmap indexes are only used for boolean columns
struct BitmapIndex { index: Vec<bool> }


impl BitmapIndex {
    pub fn search(&self, row_index_in_table: usize ) 
    -> Option<bool> { self.index.get(row_index_in_table).copied() }
}


impl Index for BitmapIndex {
    fn create_index(table_name: &str, column_index: usize) {

        let mut true_bitmap_index:  BitmapIndex = BitmapIndex{ index: Vec::new() };
        let mut false_bitmap_index: BitmapIndex = BitmapIndex{ index: Vec::new() };
        

        for page in Page::all_pages_for(table_name).unwrap() {
        
            for record in Page::all_records_in(&page).unwrap() {

                let record_data = record.data_immut();
                let value_for_index = record_data.get(column_index).unwrap(); 

                match value_for_index {
                    FieldValue::BOOL(b) => {
                        if *b { true_bitmap_index.index.push(true); false_bitmap_index.index.push(false); }
                        else  { true_bitmap_index.index.push(false); false_bitmap_index.index.push(true); }
                    },
                    FieldValue::NULL => { true_bitmap_index.index.push(false); false_bitmap_index.index.push(false); }
                    _ => { panic!("Cannot create a bitmap index on a non boolean column."); }    
                
                }
            }
        }    

        true_bitmap_index.save_index(table_name, column_index, "true");
        false_bitmap_index.save_index(table_name, column_index, "false");
            
    }


    
    fn load_index(table_name: &str, column_index: usize, col_type_name: &str) -> Self where Self: Sized {
        let file_path = Path::new(&index_dir(table_name)).join(index_file_name(table_name, column_index, col_type_name));

        let mut file = File::open(file_path).expect("Failed to open index file.");
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).expect("Failed to read index file.");

        let index: Vec<bool> = bincode::deserialize(&buffer).expect("Failed to deserialize bitmap index.");
        BitmapIndex { index }
    }



    fn save_index(&self, table_name: &str, column_index: usize, col_type_name: &str) {
        let file_path = Path::new(&index_dir(table_name)).join(index_file_name(table_name, column_index, col_type_name));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)
            .expect("Failed to open index file for writing.");

        let serialized = bincode::serialize(&self.index).expect("Failed to serialize bitmap index.");
        file.write_all(&serialized).expect("Failed to write to index file.");
    }
}