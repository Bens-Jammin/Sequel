

/*
 --- TODO ---
 (1) implement data compression and decompression
 (2) convert Option<T> returns into Result<T, DBErr>

*/

use std::{fs::OpenOptions, io::{Read, Write}, path::Path, sync::atomic::{AtomicU8, Ordering}};

use colored::Colorize;

use crate::access::locations::page_dir;

use super::record::Record;



const PAGE_SIZE: usize = 4096; // bytes
static PAGE_ID_COUNTER: AtomicU8 = AtomicU8::new(0);
fn next_page_id() -> u8 { PAGE_ID_COUNTER.fetch_add(1, Ordering::Relaxed) }





pub fn path_to_page(table_name: &str, page_id: u8) -> String {
    format!("{}/{}_{}.bin", page_dir(table_name), table_name, page_id)
}



#[derive(Debug)]
pub struct Page {
    end_of_page_ptr: usize,
    data: [u8; PAGE_SIZE]
}


impl Page {



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
    


    /// adds a record to the page. If there is an empty spot in the pages `free list`, it will be added to the first free spot. 
    /// Otherwise, the record will be added just after the last page.
    pub fn write_record(&mut self, record: Record) {
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

            /*
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
        
        // Write the record
        self.data[start_index..start_index + record_size_in_page].copy_from_slice(&record_binary);
    
        // Update page metadata
        if self.data[2] == 0 {
            self.data[2] = record_binary.len() as u8; // Set record size in the page header
        }
        self.data[1] += 1; // Increment record count
    
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
            // Update the free list to point to the current record's location
            self.data[tail_of_free_list as usize] = ((idx >> 8) & 0xFF) as u8;   // High byte of idx
            self.data[(tail_of_free_list as usize) + 1] = (idx & 0xFF) as u8;    // Low byte of idx
    
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
    


    /// Flushes the page to disc
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



    pub fn read_page(page_id: u8, table_name: &str) -> Option<Page> {
        
        let file_path = path_to_page( table_name, page_id );
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



    pub fn all_pages_for(table_name: &str, ) -> Option<Vec<Page>> {

        let mut pages: Vec<Page> = Vec::new();
        let mut page_id = 0;
        
        'load_pages: loop {
            let page_read = Page::read_page(page_id, table_name);
            
            match page_read {
                Some(p) => pages.push( p ),
                None => break 'load_pages,
            }        

            page_id += 1;
        }

        match pages.len() {
            0 => None,
            _ => Some( pages )
        }
    }



    pub fn all_records_in(&self) -> Option<Vec<Record>> {

        let mut records: Vec<Record> = Vec::new();
        let mut record_id = 0;
        
        'load_records: loop {
            let record_read = self.read_record(record_id);
            
            match record_read {
                Some(r) => records.push( r ),
                None => break 'load_records,
            }        

            record_id += 1;
        }

        match records.len() {
            0 => None,
            _ => Some( records )
        }
    }

}