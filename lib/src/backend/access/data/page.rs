

/*
 --- TODO ---
 (1) implement data compression and decompression
 (2) convert Option<T> returns into Result<T, DBErr>

*/

use std::{fs::OpenOptions, io::{Read, Write}, path::Path};
use colored::Colorize;

use crate::backend::{
    access::catalog::syscat::{
        read_syscat, 
        update_syscat_on_disk
    }, 
    utils::{
        binary::{u16_to_u8, u8_to_u16},
        files::pages_directory
    }
};
use super::record::Record;



const PAGE_SIZE: usize = 4096; // bytes



pub fn path_to_page(table_name: &str, page_id: u8) -> String {
    format!("{}/{}.bin", pages_directory(table_name), page_id)
}



#[derive(Debug)]
pub struct Page {
    data: [u8; PAGE_SIZE]
}


const HEADER_SIZE_BYTES: usize = 6;

impl Page {
    pub fn new(id: u8) -> Self {
        let mut p = Page { data: [0; PAGE_SIZE] };

        // page header details
        p.data[0] = id; 
        // the second byte (index 1) is for the number of records in a page
        // the 3rd and 4th bytes (index 2/3) hold the size of each record (bytes)
        // the 5th and 6th (index 4/5) hold the first free spot in the page
        
        p
    }


    pub fn freelist_head(&self)     -> u16 { u8_to_u16(self.data[4], self.data[5]) }
    pub fn record_size(&self)       -> u16 { u8_to_u16(self.data[2], self.data[3]) }
    pub fn number_of_records(&self) -> u8 { self.data[1] }
    pub fn id(&self)            -> u8 { self.data[0] }


    pub fn decrement_record_count(&mut self) { self.data[1] = self.number_of_records() + 1; }
    pub fn increment_record_count(&mut self) { self.data[1] = self.number_of_records() + 1; }
    pub fn set_record_size(&mut self, new_size: u16) { (self.data[2], self.data[3]) = u16_to_u8(new_size); }
    pub fn update_freelist_tail(&mut self, new_tail: u16) { (self.data[4], self.data[5]) = u16_to_u8(new_tail); }

    /// the buffer is full if it cannot fit anymore records without an overflow or overwriting existing data
    pub fn buffer_is_full(&self) -> bool {
        let record_sizes = self.record_size() as usize;
        if record_sizes == 0 { return false } // no pages yet!
        self.end_of_page() + record_sizes > PAGE_SIZE
    }
    
    pub fn view_debug(&self) {
        let size_of_record = self.record_size() as usize;
        let number_of_records = self.number_of_records() as usize;
        for (idx, byte) in self.data.iter().enumerate() {
            if idx < HEADER_SIZE_BYTES {
                // Page Header
                print!("{}", format!("{} ", byte.to_string()).on_green());
            } else {
                // Determine the current record index
                let record_idx = (idx - HEADER_SIZE_BYTES) / size_of_record;
                // Assign a color based on the record index
                let mut colored_output = match record_idx % 4 {
                    0 => format!("{} ", byte).on_red(),
                    1 => format!("{} ", byte).on_blue(),
                    2 => format!("{} ", byte).on_purple(),
                    _ => format!("{} ", byte).on_yellow(),
                };
                
                if record_idx as usize >= number_of_records { 
                    colored_output = colored_output.on_black(); 
                }
                
                if idx == self.end_of_page() {
                    colored_output = format!("{} ", byte).on_bright_magenta().blink();
                }

                // Print the byte with the assigned color
                print!("{}", colored_output);
            }
        }
        println!(); // Move to the next line after printing all numbers
    }



    /// Returns the address where a new free space can be added,
    /// or the last address of the current free list.
    fn find_end_of_free_list(&self) -> u16 {
    let freelist_address_in_header = self.freelist_head();
        

        if freelist_address_in_header == 0 { return 4; }
    
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
        let new_record_byte_length = record_binary.len();

        // Validate record size
        if self.record_size() != 0 && self.record_size() != new_record_byte_length as u16 {
            println!("\terror incoming, see record for details:\n {:?}\n or in binary... \n{:?}", &record, &record_binary);
            self.view_debug();
            panic!("\trecord is {} bytes, should be {} bytes.", new_record_byte_length, self.record_size())
        }
    
        // Check for buffer overflow
        if self.buffer_is_full() {
            panic!("page buffer overflow")
        }

    
        let start_index: usize;
    
        // Check for free space in the freelist
        let freelist_head = self.freelist_head();
        if freelist_head != 0 {
            /*
            1. get the address at the header containing the next element in the free list
            2. go to that address, get the pointer it contains and put that at the header
            3. set new data at what was the first free space
            
            *  the end of the freelist always contains a zero-value, so setting the head to zero
               is the same as "deleting" the freelist  
            */
            
            
            start_index = self.freelist_head() as usize;
            
            self.data[4] = self.data[start_index];
            self.data[5] = self.data[start_index + 1]

        } else {
            // No free spaces, write to the end of the page
            start_index = self.end_of_page();
        }
        // println!("end of page ptr going from {} --> {}", self.end_of_page_ptr, self.end_of_page_ptr + new_record_byte_length);
        
        // Write the record
        self.data[start_index..start_index + new_record_byte_length].copy_from_slice(&record_binary);
        
        // Update page metadata
        if self.record_size() == 0 {
            self.set_record_size(new_record_byte_length as u16); // Set record size in the page header
        }
        self.increment_record_count();    
    }
    


    pub fn remove_record(&mut self, record_id: u8) {
        let record_size = self.record_size();
    
        for idx in (HEADER_SIZE_BYTES..self.data.len()).step_by(record_size as usize) {
            
            if self.data[idx] != record_id {
                continue;
            }
            
            let tail_of_free_list = self.find_end_of_free_list();
    
            // Validate indices for updating the free list
            if (tail_of_free_list as usize) + 1 >= self.data.len() {
                panic!("Free list tail points outside the bounds of the page!");
            }
            
            // Update the tail of the  free list 
            // to point to the location of the record being deleted
            self.data[tail_of_free_list as usize] = ((idx >> 8) & 0xFF) as u8; // High byte of idx
            self.data[(tail_of_free_list as usize) + 1] = (idx & 0xFF) as u8;  // Low byte of idx
    
            // Clear the record's memory
            for i in idx..idx + record_size as usize {
                self.data[i] = 0;
            }
            // Decrease the record count
            if self.number_of_records() > 0 {
                self.decrement_record_count();
            } else {
                panic!("Record count underflow!");
            }
    
            return;
        }
    }
    
    
    pub fn read_record(&self, record_id: u8) -> Option<Record> { 
        let record_size = self.record_size();
        for idx in (HEADER_SIZE_BYTES..self.data.len()).step_by( record_size as usize ) {
            // only returns a recorded record if the id matches at an id index and the space which would 
            // be read is enough to contain a record. The index must also be before the end of the page
            if self.data[idx] == record_id && (idx+record_size as usize) < PAGE_SIZE && self.end_of_page() > idx { 
                return Record::from_binary(
                    self.data[idx..idx+record_size as usize].to_vec()
                );
            }
        }
        
        None 
    }
    


    /// Flushes the page to disk
    /// ## NOTE
    /// the path must be the TABLE path: .../sequel/users/<username>/<tablename>
    pub fn flush_to_disk(&self, _table_name: &str, path: &str) {
        let file_path = path_to_page( path, self.id());
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true) // overwrite data
            .open(file_path)
            .unwrap();
        file.write(&self.data).unwrap();
    }


    /// ## NOTE
    /// the path must be the TABLE path: .../sequel/users/<username>/<tablename>
    pub fn write_to_disk(&mut self, record: Record, table_name: &str, path: &str) {
        let mut syscat = read_syscat(&path).unwrap();
        
        if syscat.free_pages.len() == 0 { // add the first page
            syscat.free_pages.push(self.id());
            syscat.total_pages += 1;
        }

        // if the page can't fit any more records, then increase the record counter
        let size_with_next_record = self.end_of_page() + self.record_size() as usize;
        if size_with_next_record > PAGE_SIZE {
            syscat.total_pages += 1;
        }
        syscat.next_record_id += 1;
        update_syscat_on_disk(&syscat);
        self.write_record(record);
        self.flush_to_disk(table_name, &syscat.data_dir);
    }



    /// reads a page for a table
    /// 
    /// <b>param:</b> `page_id` (u8) : the id of the page to be read </br>
    /// <b>param:</b> `table_name` (&str) : the name of the table from which to read the page </br>
    /// <b>param:</b> `dir_to_table` (&str) : the root of the directory which contains all the tables data. Should be in the form `appdata/roaming/<table_name>` </br>
    /// 
    /// <b>returns:</b> Option<Page> : the page if there are no i/o failures
    /// 
    /// ## NOTE
    /// the path must be the TABLE path: .../sequel/users/<username>/<tablename>
    pub fn read_page(page_id: u8, _table_name: &str, path: &str) -> Option<Page> {
        
        let file_path = format!("{}/{}.bin", path, page_id);

        // 0 isn't a valid index for this. earliest page is stored as an unisigned integer in the syscat
        if page_id == 0 { return None }
        let file = OpenOptions::new()
            .read(true)
            .open( 
                Path::new(&file_path) 
            );

        match &file {
            Ok(_) => {},
            Err(_) => return None
        }
            

        let mut page_buffer: Vec<u8> = Vec::new();
        file.unwrap().read_to_end(&mut page_buffer).ok()?;
        

        for i in (0..page_buffer.len()).step_by( PAGE_SIZE ) {
            let byte = page_buffer[i];

            // when you find  the page...
            if byte as u8 == page_id {
                // copy data over
                let mut page_data = [0u8; PAGE_SIZE];
                page_data.copy_from_slice(&page_buffer[i..i+PAGE_SIZE]);
                
                return Some( Page { data: page_data } ); 
            }
        }
        None
    }



    pub fn all_pages_for(table_name: &str, dir_to_table: &str ) -> Option<Vec<Page>> {

        let mut pages: Vec<Page> = Vec::new();
        let mut page_id = 0;
        
        'load_pages: loop {
            let page_read = Page::read_page(page_id, table_name, dir_to_table);
            
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
        let number_of_records_in_page = self.number_of_records();
        let mut number_of_records_retrieved = 0; // i think this is faster than calling `.len()` every iteration

        'load_records: loop {
            let record_read = self.read_record(record_id);
            match record_read {
                Some(r) => { records.push( r ); number_of_records_retrieved += 1; },
                None => (), // do nothing
            }
            if number_of_records_retrieved == number_of_records_in_page 
            || number_of_records_in_page == 0 
                { break 'load_records }

            record_id += 1;
        }
        match records.len() {
            0 => None,
            _ => Some( records )
        }
    }


    pub fn is_full(&self) -> bool {
        // if the freelist is non-zero, then it contains at least one record.
        self.freelist_head() == 0 && self.buffer_is_full()
    }


    pub fn end_of_page(&self) -> usize {
        HEADER_SIZE_BYTES + self.number_of_records() as usize * self.record_size() as usize
    }

}




pub struct PageReader {
    index: u8,
    table_name: String,
    path_to_table: String
}


impl PageReader {
    pub fn init(table_name: &str, path_to_table: &str) -> Self 
        { PageReader{ index: 0, table_name: String::from(table_name), path_to_table: path_to_table.to_owned()} }
    pub fn next(&mut self) -> Option<Page> { self.index += 1 ; Page::read_page(self.index, &self.table_name, &self.path_to_table) }
    pub fn reset(&mut self) { self.index = 0; }
}