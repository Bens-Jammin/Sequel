use std::{collections::HashMap, fs::{self, File}, io::{self, BufRead}, path::Path, sync::atomic::{AtomicU8, Ordering}};

use crate::backend::access::{
    catalog::syscat::{self, init_syscat, read_syscat, ColumnMetaData, SystemCatalog}, data::{
        page::{Page, PageReader}, 
        record::Record, 
        value::{ColumnType, FieldValue}
    }, 
};


pub struct Table {
    pub name: String,
    pub table_dir: String,
    syscat: SystemCatalog
} 



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


impl Table {
    pub fn page_dir(&self)  -> String { format!("{}/{}/page", self.table_dir, self.name) }
    pub fn index_dir(&self) -> String { format!("{}/{}/index", self.table_dir, self.name) }
    pub fn table_datapath(&self) -> String {
        let appdata_path = std::env::var("APPDATA").unwrap();
        format!("{}/Sequel/data/{}", appdata_path, &self.name)
    }
}


pub fn pages_directory(root: &str ) -> String { format!("{}/pages", root) }
pub fn index_directory(root: &str ) -> String { format!("{}/index", root) }
pub fn table_directory(name: &str ) -> String {     
    let appdata_path = std::env::var("APPDATA").unwrap();
    format!("{}/Sequel/data/{}", appdata_path, &name)
}


// ==================================================================
//                            CREATION
// ==================================================================
impl Table {
pub fn init(table_name: String, columns: Vec<(String, (ColumnType, bool))>, path_for_tables: String) -> Table {
    
    let appdata_path = std::env::var("APPDATA").unwrap();
    let path_to_table_directory = format!("{}/Sequel/data/{}", appdata_path, &table_name);

    // if dir exists, delete it
    match fs::remove_dir_all( &path_to_table_directory ) {
        Ok(_) => (),
        Err(e) => eprintln!("error clearing table dir: {e}")
    }


    // setup folder in c:/users/.../appdata/roaming
    let path = std::env::var("APPDATA").unwrap();
    let _data_dir = format!("{}/Sequel/data/{}", path, &table_name);

    // setup folders
    fs::create_dir_all( &path_to_table_directory ).expect("Unable to create table folder");
    fs::create_dir( pages_directory(&path_to_table_directory) ).expect("Unable to create page folder for table");
    fs::create_dir( index_directory(&path_to_table_directory) ).expect("Unable to create index folder for table");
    init_syscat(&table_name, &columns, path_to_table_directory.clone());

    let syscat = read_syscat( &path_to_table_directory ).unwrap();
    Table {
        name: table_name,
        table_dir: path_for_tables,
        syscat
    }
}
}
/// the number of records required for the table to automatically be stored in main memory.
const TEMP_TABLE_SIZE_THRESHOLD: usize = 1_000_000;



// ==================================================================
//                          DESRUCTION
// ==================================================================


// ==================================================================
//                           INSERTION
// ==================================================================


impl Table {

pub fn add_column(&mut self, column: (String, (ColumnType, bool)) ) {
    // panic if there are any rows in the table. to be added later
    if self.syscat.next_record_id > 1 { panic!("Cannot (yet) add a column toa table with existing rows") }
    
    let (name, (data_type, allows_nulls)) = column;
    self.syscat.columns.push( ColumnMetaData { name, data_type, allows_nulls });
}



// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//  TODO: maybe set the data_dir to be the whole director in \data
//  then adjust the page_save_location in page::readpage to use the directory made
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

pub fn insert_row(&mut self, row: Vec<FieldValue>) -> &mut Self {
    
    let data = Record::new(row);
    if (self.syscat.total_pages == 0) || self.syscat.free_pages.len() == 0 {
        let mut p = Page::new( self.syscat.next_page_id );
        self.syscat.total_pages += 1;
        self.syscat.next_record_id += 1;
        self.syscat.next_page_id += 1;
        self.syscat.free_pages.push( p.id() );
        p.write_to_disc(data, &self.name);
        return self;
    }
    
    let free_page_id = self.syscat.free_pages[0];
    let mut page = Page::read_page(free_page_id, &self.name, &self.page_dir()).unwrap();
    page.write_to_disc(data, &self.name);
    if page.is_full() {
        // remove page id from syscat
        self.syscat.free_pages.remove(
            self.syscat.free_pages.iter().position(
                |v| *v == free_page_id
            ).unwrap()
        );
    }
    self.syscat.next_record_id += 1;
    self
}






pub fn bulk_insert_records(&mut self, rows: Vec<Record>) -> &mut Self {
    for row in rows { Self::insert_record(&self.name, row, &mut self.syscat); }
    self
}

fn insert_record(table_name: &str, data: Record, syscat: &mut SystemCatalog) {

    let page_dir = pages_directory( &table_directory(table_name) );

    if (syscat.total_pages == 0) || syscat.free_pages.len() == 0 {
        let mut p = Page::new( syscat.next_page_id );
        p.write_to_disc( data, table_name );
        syscat.total_pages += 1;
        syscat.next_record_id += 1;
        syscat.next_page_id += 1;
        syscat.free_pages.push( p.id() );
        return;
    }

    let free_page_id = syscat.free_pages[0];
    let mut page = Page::read_page(free_page_id, table_name, &page_dir ).unwrap();
    page.write_to_disc(data, table_name );
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

}

// ==================================================================
//                            READING
// ==================================================================


pub fn load_blocks_from_start(table_name: &str, dir: &str) -> [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] {
    reset_page_counter();
    reset_record_counter();
    load_next_block(table_name, dir)
}


/// reads the next block of records in a page
/// <b>param:</b> `table_name` (&str) : the name of the table to read from </br>
/// <b>param:</b> `dir` (&str) : the root directory of the table </br>
/// <b>returns:</b> an array of size `NUMBER_OF_RECORDS_IN_BLOCK`. Contents of the array are all otional records, in case the size of the array is bigger than the number of records remaining </br>
pub fn load_next_block(table_name: &str, dir: &str) -> [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] { 

    let mut records: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] = std::array::from_fn(|_| None);
    let mut count = 0;

    let number_of_pages_in_table = read_syscat(dir).unwrap().total_pages;

    // no blocks to load if there aren't any pages
    if number_of_pages_in_table == 0 { return records; }

    // if the counter hasn't been reset, do so and start from the beginning
    if number_of_pages_in_table <= next_page_id_to_get() as u16 { 
        reset_page_counter(); 
        reset_record_counter(); 
        return load_blocks_from_start(table_name, dir)
    }


    let mut iter = PageReader::init(&table_name, dir);

    let _ = match iter.next() {
        Some(p) => p,
        None => { reset_page_counter(); return records }
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

fn evaluate_condition(condition: &Condition, cell_value: &FieldValue) -> bool {
    
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

impl Table {
/// Creates a new table that is filtered on the original. Returns the filtered table.
pub fn filter_table(&mut self, col: &str, condition: Condition ) -> Table {
    
    let mut accepted_records: Vec<Record> = Vec::new();
    let mut new_index: u8 = 0;

    let columns: Vec<&ColumnMetaData> = self.syscat.columns.iter().clone().collect();

    let column_index_in_record = (&columns)
        .iter()
        .position(
            |c| 
            c.name == col
        ).unwrap();

    for mut record in TableIterator::init( &self.name ) {
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
    let new_name = generate_new_name(&self.name);
    let columns_for_syscat = columns
        .iter()
        .map(|metadata| (metadata.name.clone(), (metadata.data_type.clone(), metadata.allows_nulls)))
        .collect::<Vec<(String, (ColumnType, bool))>>();

    let _number_of_rows_remaining = accepted_records.len();
    let dir = &self.table_datapath();
    let mut filtered_table = Table::init( String::from(&new_name), columns_for_syscat, dir.clone() ); 

    filtered_table.bulk_insert_records(accepted_records);
    // number_of_rows_remaining
    filtered_table
}
}


fn generate_new_name(table: &str) -> String {
    format!("filtered {}", table)
}


// ==================================================================
//                            SORTING
// ==================================================================


// ...


// ==================================================================
//                         IMPORT / EXPORT
// ==================================================================



impl Table{

pub fn  from_csv(path: &str, save_location: &str) -> Option<Table> {

    let file: File = File::open(path).ok()?;
    let reader = io::BufReader::new(file);
    let table_name: &str = &Path::new(path)
        .file_name()?
        .to_str()?
        .replace(".csv", "");

    let mut table = Table::init(table_name.to_owned(), Vec::new(), save_location.to_owned() );

    
    // populating the new
    let mut rows: Vec<Vec<FieldValue>> = Vec::new();
    let mut header_row: Vec<String> = Vec::new();
    let mut inferred_types: HashMap<String, Option<ColumnType>> = HashMap::new();  

    // phase 1: parsing data
    for (i, line) in reader.lines().enumerate() {
        let data = line.ok()?;
        let cells: Vec<String> = data.split(',').map(|s| s.to_string()).collect();
        let mut row: Vec<FieldValue> = Vec::with_capacity( cells.len() );

        // check if the row is currently the header
        if i == 0 {
            header_row.extend(cells.clone());
            for cell in cells {
                inferred_types.insert( String::from(cell), None );
            }
        // otherwise...
        } else {
            for (cell, col_name) in cells.iter().zip(&header_row) {
                let cell_type = FieldValue::parse(cell);
                row.push( cell_type.clone() );

                // infer the most lenient type for the given column and update in the hashmap
                let current_inferred_type = match inferred_types.get(&**col_name) {
                    Some(Some(v)) => v.clone(),
                    _ => cell_type.column_type(),
                };
                let upgraded_type = ColumnType::upgrade(Some(&current_inferred_type), &cell_type);
                inferred_types.insert(col_name.to_string(), Some(upgraded_type) );
            }
            rows.push( row );
        }
    }

    // insert columns before the rows
    for (_idx, header_title) in header_row.iter().enumerate() {
        
        let column_type = inferred_types.get(&**header_title).unwrap().clone().unwrap();
        
        table.add_column( ((&header_title).to_string(), (column_type, false)) );

    }


    // change the hashmap to be accessible by index rather than by column name, which makes it easier for inserting rows
    let mut column_types: HashMap<usize, ColumnType> = HashMap::new();
    for (i, column) in header_row.iter().enumerate() {
        let col_type = inferred_types.get(column).unwrap().clone().unwrap();
        column_types.insert(i, col_type);
    }
    // insert everything after the row index was found
    for row in rows { 
        let mut row_with_updated_types: Vec<FieldValue> = Vec::with_capacity( row.capacity() );
        for (i, cell) in row.iter().enumerate() {
            let cell_type = column_types.get(&i).unwrap();
            let new_cell = cell.convert_to(cell_type);
            row_with_updated_types.push( new_cell );
        }  
        table.insert_row( row_with_updated_types ); 
    }
    syscat::update_syscat_on_disk( &table.syscat );
    Some(table)
}
}


// ==================================================================
//                            INDEXING
// ==================================================================


pub fn create_index(_table: &str, _col: &str) {
    // let syscat = read_syscat(table).unwrap();
    // let column_data = syscat.columns.iter().find(|c| c.name == col).unwrap();

    // match column_data.data_type {
    //     ColumnType::NUMBER => init_bplus_index(table, col),
    //     ColumnType::FLOAT => init_bplus_index(table, col),
    //     ColumnType::STRING => panic!("not implemented yet"),
    //     ColumnType::BOOLEAN => init_bitmap_index(table, col, &syscat),
    // }
}

pub fn debug_get_index(_table: &str, _col: &str) {
    // let syscat = read_syscat(table).unwrap();
    // let column_data = syscat.columns.iter().find(|c| c.name == col).unwrap();

    // match column_data.data_type {
    //     ColumnType::BOOLEAN => bitmap(table, col),
    //     _ => panic!("not implemented yet.")
    // }
}



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
    path_to_table: String,

    buf_index: usize,
    buf: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK]
}


impl<'a> TableIterator<'a> {
    pub fn init(tablename: &'a String) -> Self {
        let path_to_table = table_directory(&tablename);
        let buffer: [Option<Record>; NUMBER_OF_RECORDS_IN_BLOCK] = std::array::from_fn(|_| None);
        Self { tablename, index: 0, buf_index: 0, buf: buffer, path_to_table }
    }   

    pub fn index(&self) -> u64 { self.index }
    pub fn item(&'a self) -> &'a Option<Record> { &self.buf[self.buf_index] }
}


impl<'a> Iterator for TableIterator<'a> {
    type Item = Record;
 
    fn next(&mut self) -> Option<Self::Item> {
        
        // load fresh data if there's nothing yet or you've exhausted the current batch
        if self.buf_index == 0 || self.buf_index >= NUMBER_OF_RECORDS_IN_BLOCK { 
            self.buf = load_next_block( &self.tablename, &self.path_to_table );
            self.buf_index = 0;
        }

        let r = self.buf[ self.buf_index ].clone();
        self.buf_index += 1;
        self.index += 1;
        r
    }
}