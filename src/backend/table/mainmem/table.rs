use std::{fs, sync::atomic::{AtomicU8, Ordering}};

use crate::backend::access::{
    catalog::syscat::{init_syscat, read_syscat, SystemCatalog}, 
    data::{
        page::{Page, PageReader}, 
        record::Record, 
        value::{ColumnType, FieldValue}
    }, 
    locations::{index_dir, page_dir, table_dir}
};




// ==================================================================
//                            CONSTANTS
// ==================================================================



static PAGE_ID_COUNTER: AtomicU8 = AtomicU8::new(0);
fn inc_next_page_id() -> u8     { PAGE_ID_COUNTER.fetch_add(1, Ordering::Relaxed) }
fn next_page_id_to_get() -> u8  { PAGE_ID_COUNTER.load(Ordering::Relaxed) }
fn reset_page_counter()         { PAGE_ID_COUNTER.store(0, Ordering::Relaxed); }

static RECORD_ID_COUNTER: AtomicU8 = AtomicU8::new(0);
fn inc_next_record_id() -> u8     { RECORD_ID_COUNTER.fetch_add(1, Ordering::Relaxed) }
fn next_record_id_to_get() -> u8  { RECORD_ID_COUNTER.load(Ordering::Relaxed) }
fn reset_record_counter()         { RECORD_ID_COUNTER.store(0, Ordering::Relaxed); }

pub const NUMBER_OF_RECORDS_IN_BLOCK: usize = 1500;



// ==================================================================
//                            CREATION
// ==================================================================

pub fn init_table(table_name: String, columns: Vec<(String, (ColumnType, bool))>) {
    
    // if dir exists, delete it
    match fs::remove_dir_all(table_dir(&table_name)) {
        Ok(_) => (),
        Err(e) => eprintln!("error clearing table dir: {e}")
    }

    // setup folders
    fs::create_dir( table_dir(&table_name)).expect("Unable to create table folder");
    fs::create_dir( page_dir(&table_name) ).expect("Unable to create page folder for table");
    fs::create_dir( index_dir(&table_name)).expect("Unable to create index folder for table");
    init_syscat(&table_name, &columns );

}

/// the number of records required for the table to automatically be stored in main memory.
const TEMP_TABLE_SIZE_THRESHOLD: usize = 1_000_000;



// ==================================================================
//                          DESRUCTION
// ==================================================================


// ==================================================================
//                           INSERTION
// ==================================================================


pub fn insert_row(table_name: &str, row: Vec<FieldValue>) {

    let mut syscat: SystemCatalog = read_syscat(table_name).unwrap();
    let data = Record::new(row);
    
    if (syscat.total_pages == 0) || syscat.free_pages.len() == 0 {
        let mut p = Page::new( syscat.next_page_id );
        syscat.total_pages += 1;
        syscat.next_record_id += 1;
        syscat.next_page_id += 1;
        syscat.free_pages.push( p.id() );
        p.write_to_disc( data, table_name );
        return;
    }

    let free_page_id = syscat.free_pages[0];
    let mut page = Page::read_page(free_page_id, table_name).unwrap();
    page.write_to_disc(data, table_name);
    if page.is_full() {
        // remove page id from syscat
        syscat.free_pages.remove(
            syscat.free_pages.iter().position(
                |v| *v == free_page_id
            ).unwrap()
        );
        syscat.next_page_id += 1;
    }
    syscat.next_record_id += 1;
}


fn insert_record(table_name: &str, data: Record, syscat: &mut SystemCatalog) {

    if (syscat.total_pages == 0) || syscat.free_pages.len() == 0 {
        let mut p = Page::new( syscat.next_page_id );
        p.write_to_disc( data, table_name );
        syscat.total_pages += 1;
        syscat.next_record_id += 1;
        syscat.next_page_id += 1;
        syscat.free_pages.push( p.id() );
        println!("early return!");
        return;
    }

    let free_page_id = syscat.free_pages[0];
    let mut page = Page::read_page(free_page_id, table_name).unwrap();
    println!("writing data to {:?}", page.id());
    page.write_to_disc(data, table_name);
    if page.is_full() {
        // remove page id from syscat
        syscat.free_pages.remove(
            syscat.free_pages.iter().position(
                |v| *v == free_page_id
            ).unwrap()
        );
        syscat.next_page_id += 1;
    }
    syscat.next_record_id += 1;
}


pub fn bulk_insert_records(table_name: &str, rows: Vec<Record>) {

    let mut syscat: SystemCatalog = read_syscat(table_name).unwrap();
    for row in rows { insert_record(table_name, row, &mut syscat); }
}


// ==================================================================
//                            READING
// ==================================================================


pub fn load_blocks_from_start(table_name: &str) -> [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] {
    reset_page_counter();
    reset_record_counter();
    load_next_block(table_name)
}



pub fn load_next_block(table_name: &str) -> [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] { 

    let mut records: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] = std::array::from_fn(|_| None);
    let mut count = 0;

    let number_of_pages_in_table = read_syscat(table_name).unwrap().total_pages;

    // no blocks to load if there aren't any pages
    if number_of_pages_in_table == 0 { return records; }

    // if the counter hasn't been reset, do so and start from the beginning
    if number_of_pages_in_table <= next_page_id_to_get() as u16 { 
        reset_page_counter(); 
        reset_record_counter(); 
        return load_blocks_from_start(table_name)
    }


    let mut iter = PageReader::init(table_name);

    let _ = match iter.next() {
        Some(p) => p,
        None => { reset_page_counter(); reset_record_counter(); return records }
    };
    iter.reset();


    while let Some(page) = iter.next() {
        if page.id() < next_page_id_to_get() { continue; }
        if let Some(page_records) = page.all_records_in() {
            for record in page_records {
                if record.id() < next_record_id_to_get() { continue; }
                if count >= NUMBER_OF_RECORDS_IN_BLOCK {
                    return records;
                }
                records[count] = Some(record);
                count += 1;
                inc_next_record_id();
            }
        }
        reset_record_counter();
        inc_next_page_id();
    }

    records
}


pub fn get_record(_record_id: usize) -> Option<Record> {
    // returns the record with the given id
    None
}


pub fn get_cell(_col_name: &str, _record: &Record) -> Option<FieldValue> {
    // returns the cell at the column given the record id
    None
}


// ==================================================================
//                          FILTRATION
// ==================================================================


pub enum Condition {
    Equals(FieldValue),
    NotEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
    GreaterThan(FieldValue),
    GreaterThanOrEqual(FieldValue),
    IsNull,
    IsNotNull,
}

pub fn evaluate_condition(condition: &Condition, cell_value: &FieldValue) -> bool {
    
    match condition {
        Condition::Equals(value)             => cell_value == value,
        Condition::NotEqual(value)           => cell_value != value,
        Condition::LessThan(value)           => cell_value  < value,
        Condition::LessThanOrEqual(value)    => cell_value <= value,
        Condition::GreaterThan(value)        => { println!("{} > {} = {}", cell_value, value, cell_value > value); cell_value  > value},
        Condition::GreaterThanOrEqual(value) => cell_value >= value,
        Condition::IsNull                                 => cell_value == &FieldValue::NULL,
        Condition::IsNotNull                              => cell_value != &FieldValue::NULL,
    }
}


/// Creates a new table that is filtered on the original. Returns the size of the new table.
pub fn filter_table(table_name: String, col: &str, condition: Condition ) -> usize {
    
    let mut accepted_records: Vec<Record> = Vec::new();
    let mut new_index: u8 = 0;

    let columns = read_syscat(&table_name).unwrap().columns;

    let column_index_in_record = (&columns)
        .iter()
        .position(
            |c| 
            c.name == col
        ).unwrap();
    for mut record in TableIterator::init( &table_name ) {
        let cell_value = record.data_immut()[column_index_in_record].clone();
        
        if evaluate_condition(&condition, &cell_value) {
            record.reassign_id(new_index);  // the id from the original will (almost) never be the same in the filter!
            new_index += 1; 
            accepted_records.push(record); 
        }
    }

    if accepted_records.len() < TEMP_TABLE_SIZE_THRESHOLD {
        // Create a struct table 
    }

    // ---------------------- saving the table ---------------------------------
    
    // how do i find the number of tables with the same base name, then make a counter | table_name (1)
    let new_name = generate_new_name(&table_name);
    let columns_for_syscat = columns
        .iter()
        .map(|metadata| (metadata.name.clone(), (metadata.data_type.clone(), metadata.allows_nulls)))
        .collect::<Vec<(String, (ColumnType, bool))>>();

    let number_of_rows_remaining = accepted_records.len();
    init_table( String::from(&new_name), columns_for_syscat ); 

    bulk_insert_records( &new_name, accepted_records);
    number_of_rows_remaining
}



fn generate_new_name(table: &str) -> String {
    format!("filtered {}", table)
}


// ==================================================================
//                            SORTING
// ==================================================================


// ...


// ==================================================================
//                            JOINING
// ==================================================================


// ...


// ==================================================================
//                         MISCELLANEOUS
// ==================================================================


pub struct TableIterator<'a> {
    tablename: &'a String,
    index: u64,

    buf_index: usize,
    buf: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK]
}


impl<'a> TableIterator<'a> {
    pub fn init(table_name: &'a String) -> Self {
        let buffer: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] = std::array::from_fn(|_| None);
        Self { tablename: table_name, index: 0, buf_index: 0, buf: buffer }
    }   

    pub fn index(&self) -> u64 { self.index }
    pub fn item(&'a self) -> &'a Option<Record> { &self.buf[self.buf_index] }
}


impl<'a> Iterator for TableIterator<'a> {
    type Item = Record;
 
    fn next(&mut self) -> Option<Self::Item> {
        
        // load fresh data if there's nothing yet or you've exhausted the current batch
        if self.buf_index == 0 || self.buf_index >= NUMBER_OF_RECORDS_IN_BLOCK { 
            self.buf = load_next_block( &self.tablename );
            self.buf_index = 0;
        }

        let r = self.buf[ self.buf_index ].clone();
        self.buf_index += 1;
        self.index += 1;
        r
    }
}
