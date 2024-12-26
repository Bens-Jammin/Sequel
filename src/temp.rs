
/*
    === TEMP FILE ===

    This is to store the logic for a record and page file management system
    while I implement it in the database.


*/

use std::{
    fs::OpenOptions, 
    io::{Read, Write},
     path::Path, 
     sync::atomic::{AtomicU8, Ordering}
};
use colored::*;

/*

    --- PAGE AND RECORD CONSTANTS ---

*/

const PAGE_SIZE: usize = 4096; // bytes
static page_id_counter: AtomicU8 = AtomicU8::new(0);
fn next_page_id() -> u8 { page_id_counter.fetch_add(1, Ordering::Relaxed) }


static record_id_counter: AtomicU8 = AtomicU8::new(0);
fn next_record_id() -> u8 { record_id_counter.fetch_add(1, Ordering::Relaxed) }

const PAGE_DIRECTORY: &str = "C:/Programming/Rust/db_test";


pub fn path_to_page(table_name: &str, page_id: u8) -> String {
    format!("{}/{}_{}.bin", PAGE_DIRECTORY, table_name, page_id)
}


#[derive(Debug)]
pub enum FieldValue {
    NUMBER(u32),
    FLOAT(f32),
    STRING(String),
    BOOL(bool),
    NULL
}

impl FieldValue {
    pub fn to_bitmap(&self) -> u8 {
        match &self {   // storing the FV representation as a bit map bc i dont want to deal with sizes < 8 bits
            Self::NULL      => return 0b00000000,  
            Self::NUMBER(_) => return 0b00000001,
            Self::FLOAT(_)  => return 0b00000010,
            Self::STRING(_) => return 0b00000100,
            Self::BOOL(_)   => return 0b00001000,
        }
    }

    pub fn serialize(&self) -> Option<Vec<u8>> {
        match &self {
            Self::NULL               => return Some(vec![]),
            Self::FLOAT(v)     => return Some((*v).to_ne_bytes().to_vec()),
            Self::STRING(s) => 
                if s.len() <= 20 { 
                    let mut d = s.clone().into_bytes();
                    d.append( 
                        &mut (0..(20-d.len())) // find the size of 0's needed to get to 20 chars
                        .map(|_| 0) // fill them with zeros
                        .collect::<Vec<u8>>()  // collect into a vector and append to `d`
                    );
                    return Some( d ) 
                } 
                else { return None },
            Self::BOOL(v)     => return Some(vec![*v as u8]),
            Self::NUMBER(v)    => return Some(vec![
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 08) as u8,
                *v        as u8
            ])
        }  
    }

}
pub fn deserialize(data: Vec<u8>) -> Option<FieldValue> {
    match data[0] { // type flag
        0b00000000 => return Some(FieldValue::NULL),
        0b00000001 => return Some(FieldValue::NUMBER( 
            ((data[1] as u32) << 24) 
          + ((data[2] as u32) << 16)
          + ((data[3] as u32) << 08)
          +  (data[4] as u32       ))), 
        0b00000010 => return Some(FieldValue::FLOAT( 
            f32::from_ne_bytes([data[1], data[2], data[3], data[4]]) 
        )),
        0b00000100 => {
            if data[1..].len() > 20 { return  None; } // Strings must be 20 chars or less
            let mut result_string = data[1..]
                .iter()
                .map(|byte| 
                    *byte as char
                ).collect::<String>();

            // remove the padded 0's from the serialization
            result_string = result_string.replace('\0', "");

            return Some(FieldValue::STRING( result_string )) 
        },
        0b00001000 => return Some(FieldValue::BOOL(data[1] != 0)),
        _ => return None
    }
}

#[repr(u8)]
enum DataType {
    NUMBER,
    FLOAT,
    STRING,
    BOOL
}

#[derive(Debug)]
pub struct Record { 
    id: u8,
    data: Vec<FieldValue>
}

impl Record {
    pub fn new(data: Vec<FieldValue> ) -> Self {

        let record_id = next_record_id();
        for datum in &data {
            match datum {
                FieldValue::STRING(v) => { if v.len() > 20 { panic!("No support for Strings longer than 20 chars yet.") } },
                _ => continue
            }
        }

        Record { id: record_id, data }
    }
    
    pub fn to_binary(&self) -> Vec<u8> { 
        let mut serialized_data: Vec<u8> = vec![ self.id ]; // don't forget to add the records id!
        for datum in &self.data {
            let type_flag = datum.to_bitmap();
            let mut serialized = datum.serialize().unwrap();
            serialized_data.push( type_flag );
            serialized_data.append( &mut serialized ); 
        }
        serialized_data
    }

    pub fn from_binary(data: Vec<u8>) -> Option<Record> {
        
        let mut values: Vec<FieldValue> = Vec::new();
        let record_id = data[0];
        let mut skip_to = 1;

        for (idx, datum) in data.iter().enumerate() {
            if idx < skip_to { continue; }
            match datum { // type flag
                0b00000000 => { // --- NULL ---
                    skip_to = idx + 1;
                    values.push( FieldValue::NULL )
                },
                0b00000001 => { // --- NUMBER ---
                    skip_to = idx + 5;
                    values.push( deserialize( data[idx..=idx+04].to_vec() ).unwrap() )
                },
                0b00000010 => { // --- FLOAT ---
                    skip_to = idx + 5;
                    values.push( deserialize( data[idx..=idx+04].to_vec() ).unwrap() )
                },
                0b00000100 => { // --- STRING ---
                    skip_to = idx + 21;
                    let string_data = data[idx..=20].to_vec();
                    values.push( deserialize( string_data ).unwrap() )
                },
                0b00001000 => { // --- BOOL ---
                    skip_to = idx + 2;
                    values.push( FieldValue::BOOL( data[idx+1] != 0 ) )
                },
                _ => return None
            }
        } 
        
        Some( Record { id: record_id, data: values } )
    }
}



#[derive(Debug)]
pub struct Page {
    end_of_page_ptr: usize,
    data: [u8; PAGE_SIZE]
}


impl Page {

    // TODO: (later) compression!!
    pub fn id(&self) -> u8 { self.data[0] }
    pub fn new() -> Self {
        let mut p = Page { end_of_page_ptr: 5, data: [0; PAGE_SIZE] };

        // page header details
        p.data[0] = next_page_id(); 
        p.data[1] = 0;          // current number of records in the page
        p.data[2] = 0;          // when the first record is written, this will hold the size of each record in bytes 
        p.data[3] = 0;          // if a page is removed, this is the beginning of the freelist to track empty slots
        p.data[4] = 0;          // using 2 bits because 4096 bits can't be stored in a `u8`
        p
    }
    
    
    pub fn view_debug(&self) {
        let page_header_size = 5; // Assuming the first 4 bytes are the page header
        let size_of_record = self.data[2] as usize;
        let number_of_records = self.data[1] as usize;
    
        for (idx, byte) in self.data.iter().enumerate() {
            if idx < page_header_size {
                // CASE 1: Page Header
                print!("{}", format!("{} ", byte.to_string()).on_green());
            } else {
                // Determine the current record index
                let record_idx = (idx - page_header_size) / size_of_record;
    
                // Assign a color based on the record index
                let mut colored_output = match record_idx % 4 {
                    0 => format!("{} ", byte).on_red(),
                    1 => format!("{} ", byte).on_blue(),
                    2 => format!("{} ", byte).on_purple(),
                    _ => format!("{} ", byte).on_yellow(),
                };
    
                if record_idx as usize > number_of_records { 
                    colored_output = colored_output.on_black(); 
                }

                // Print the byte with the assigned color
                print!("{}", colored_output);
            }
        }
        println!(); // Move to the next line after printing all numbers
    }


    /// Returns the first address for the end of the free list in the page. 
    /// 2 bytes are always allocated for the free list
    fn find_end_of_free_list(&self) -> u16 {
        let freelist_address_in_header = ((self.data[3] as u16) << 8) + self.data[4] as u16;
        
        // No free spaces
        if freelist_address_in_header == 0 { return 3; }
    
        let mut free_address = freelist_address_in_header;
    
        while self.data[free_address as usize] != 0 {
            if free_address as usize >= self.data.len() - 1 {
                panic!("freelist went too far and is now beyond the page size!");
            }
    
            // Read the next free address from the array
            free_address = 
                ((self.data[free_address as usize] as u16) << 8)
                + self.data[(free_address as usize) + 1] as u16;
        }
        
        free_address
    }
    

    pub fn write_record(&mut self, record: Record) -> bool {
        let record_binary = record.to_binary();
        let record_size_in_page = record_binary.len();
    
        // Validate record size
        if self.data[2] != 0 && self.data[2] != record_binary.len() as u8 {
            panic!("record is {} bytes, should be {} bytes.", record_binary.len(), self.data[2])
        }
    
        // Check for buffer overflow
        if self.end_of_page_ptr + record_size_in_page > PAGE_SIZE {
            panic!("buffer overflow")
        }
    
        let start_index: usize;
    
        // Check for free space in the freelist
        let freelist_head = ((self.data[3] as u16) << 8) + self.data[4] as u16;
        if freelist_head != 0 {

            /* TODO:
            1. get the address at the header containing the next element in the free list
            2. go to that address, get the pointer it contains and put that at the header
            3. set new data at what was the first free space
            */

            // step 1
            start_index = ((self.data[3] as usize) << 8) + self.data[4] as usize;
            
            self.data[3] = self.data[start_index];
            self.data[4] = self.data[start_index + 1]

        } else {
            // No free spaces, write to the end of the page
            start_index = self.end_of_page_ptr;
            self.end_of_page_ptr += record_size_in_page;
        }
        
        println!("new record start = {}", start_index);
        // Write the record
        self.data[start_index..start_index + record_size_in_page].copy_from_slice(&record_binary);
    
        // Update page metadata
        if self.data[2] == 0 {
            self.data[2] = record_binary.len() as u8; // Set record size in the page header
        }
        self.data[1] += 1; // Increment record count
    
        true
    }
    

    pub fn remove_record(&mut self, record_id: u8) {
        let record_size = self.data[2];
    
        for idx in (5..self.data.len()).step_by(record_size as usize) {
            if self.data[idx] != record_id {
                continue;
            }
            
            let tail_of_free_list = self.find_end_of_free_list();
    
            // Validate indices for updating the free list
            if (tail_of_free_list as usize) + 1 >= self.data.len() {
                panic!("Free list tail points outside the bounds of the page!");
            }
            println!("data[1] = {} | tail of free list = {}", self.data[1], tail_of_free_list);    
            // Update the free list to point to the current record's location
            self.data[tail_of_free_list as usize] = ((idx >> 8) & 0xFF) as u8;   // High byte of idx
            self.data[(tail_of_free_list as usize) + 1] = (idx & 0xFF) as u8;    // Low byte of idx
            println!("data[1] = {}", self.data[1]);
    
            // Clear the record's memory
            for i in idx..idx + record_size as usize {
                self.data[i] = 0;
            }
            // Decrease the record count
            if self.data[1] > 0 {
                self.data[1] -= 1;
            } else {
                panic!("Record count underflow!");
            }
    
            return;
        }
    }
    

    
    pub fn read_record(&self, record_id: u8) -> Option<Record> { 
        
        let record_size = self.data[2];
        for idx in (5..self.data.len()).step_by( record_size as usize ) {
            if self.data[idx] == record_id { 
                return Record::from_binary(
                    self.data[idx..idx+record_size as usize].to_vec()
                );
            }
        }
        
        None 
    }
    

    pub fn flush(&self) {
        let table_name = "sample_relation_name";
        let file_path = path_to_page( table_name, self.id() );
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open( file_path )
            .unwrap();
        file.write( &self.data ).unwrap();

    }
}

// TODO: turn most of the Option< T > into Results
pub fn read_page_from_file(page_id: u8, table_name: &str) -> Option<Page> {
    
    let file_path = path_to_page( table_name, page_id );
    println!("path is '{file_path}'");
    let mut file = OpenOptions::new()
        .read(true)
        .open( 
            Path::new(&file_path) 
        )
        .unwrap();
    let mut page_buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut page_buffer).ok()?;


    for i in (0..page_buffer.len()).step_by( PAGE_SIZE ) {
        let byte = page_buffer[i];
        if byte as u8 == page_id {
            let number_of_records_in_page = page_buffer[i + 1];
            let size_of_records = page_buffer[i + 2]; 
            
            // copy data over
            let mut page_data = [0u8; PAGE_SIZE];
            page_data.copy_from_slice(&page_buffer[i..i+PAGE_SIZE]);

            return Some(Page { 
                end_of_page_ptr: number_of_records_in_page as usize * size_of_records as usize,
                data: page_data
            });
        }
    }

    None
}


fn main() {

    // Create a new page
    let mut page = Page::new();

    // Create and write a few sample records
    let record1 = Record::new(vec![
        FieldValue::NUMBER(42),
        FieldValue::STRING("Alice".to_string()),
        FieldValue::BOOL(true),
    ]);
    let record2 = Record::new(vec![
        FieldValue::NUMBER(84),
        FieldValue::STRING("Bob".to_string()),
        FieldValue::BOOL(false),
    ]);
    let record3 = Record::new(vec![
        FieldValue::NUMBER(126),
        FieldValue::STRING("Charlie".to_string()),
        FieldValue::BOOL(true),
    ]);
    let record4 = Record::new(vec![
        FieldValue::NUMBER(255),
        FieldValue::STRING("Danielle".to_string()),
        FieldValue::BOOL(false),
    ]);
    let record5 = Record::new(vec![
        FieldValue::NUMBER(1),
        FieldValue::STRING("Elliott".to_string()),
        FieldValue::BOOL(true),
    ]);

    page.write_record(record1);
    page.write_record(record2);
    page.write_record(record3);
    page.write_record(record4);

    println!("end of free list = {:?}", page.find_end_of_free_list());
    println!("Before removal:");
    println!("number of records = {}", page.data[1]);
    for record_id in 0..4 {
        print!("searching for record #{record_id}...   ");
        if let Some(record) = page.read_record(record_id) {
            println!("Record {}: {:?}", record_id, record);
        }
    }

    // Remove a record
    println!("\nRemoving record with ID 1...");
    page.remove_record(1);
    println!("\nAfter removal:");
    println!("number of records = {}", page.data[1]);
    for record_id in 0..4 {
        if let Some(record) = page.read_record(record_id) {
            println!("Record {}: {:?}", record_id, record);
        } else {
            println!("Record {}: None", record_id);
        }
    }

    // Remove a record
    println!("\nRemoving record with ID 2...");
     page.remove_record(2);
    println!("\nAfter removal:");
    println!("number of records = {}", page.data[1]);
    for record_id in 0..4 {
        if let Some(record) = page.read_record(record_id) {
            println!("Record {}: {:?}", record_id, record);
        } else {
            println!("Record {}: None", record_id);
        }
    }
    
    page.write_record( record5 );

    println!("\n after adding a new record...");
    println!("number of records = {}", page.data[1]);
    for record_id in 0..=4 {
        if let Some(record) = page.read_record(record_id) {
            println!("Record {}: {:?}", record_id, record);
        } else {
            println!("Record {}: None", record_id);
        }
    }
    
    page.view_debug();
}