use std::{cmp::Ordering, collections::{BTreeMap, HashMap}, fs::{File, OpenOptions}, io::{Read, Write}, usize};
use rand::seq::index;
use serde::{Deserialize, Serialize};
use bincode;
use crate::{config, structures::{column::{Column, DataType, FieldValue}, db_err::DBError, modify_where::FilterCondition, sort_method::SortCondition}};


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    primary_keys: Vec<Column>,
    /// ik this implementation is incredibly bad and slow,
    /// what are you gonna do about it?
    rows: Vec<HashMap<String, FieldValue>>
}

/// ====================================================================================
/// TODO: 
/// * (TOP PRIORITY) learn how to cache values (such as the index and relation paths)
/// * (TOP PRIORITY) learn how to use the config file in other projects (i.e. the server)
/// * (TOP PRIORITY) test index efficiency with different sized tables
/// * (MED PRIORITY) test config file in database
/// * (MED PRIORITY) test indexing (creation, saving, loading)
/// * (LOW PRIORITY) allow exporting / importing from csv, excel files
/// =====================================================================================


// |===========================|
// |     utility functions     |
// |===========================|

impl Table {
    pub fn rows(&self) -> &Vec<HashMap<String, FieldValue>> { &self.rows }

    pub fn get_row(&self, row_index: u64) -> Option<&HashMap<String, FieldValue>> { self.rows.get(row_index as usize) }

    pub fn columns(&self) -> &Vec<Column> { &self.columns }

    pub fn all_column_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for c in &self.columns { names.push( c.get_name().to_string() ); }
        names
    } 
    
    pub fn primary_keys(&self) -> &Vec<Column> { &self.primary_keys }
    
    
    /// determines if a column with the given name exists in the database.
    /// 
    /// returns a Some value containing a clone of the column if it exists.
    pub fn column(&self, col_name: String) -> Option<Column> {
        for c in &self.columns {
            if c.get_name() == col_name { return Some( c.clone() ) }
        }
        None
    }


    /// determines if a primary key with the given name exists in the database.
    /// 
    /// return a Some value containing a clone of the column if it exists.
    pub fn primary_key(&self, pk_name: String) -> Option<Column> {
        for c in &self.columns {
            if c.get_name() == pk_name { return Some( c.clone() ) }
        }
        None
    }


    pub fn is_valid_column(&self, col_name: &String) -> bool { self.column(col_name.to_string()).is_some() }
    
        
    /// determines if the given column name is the name of a valid primary key column in the database
    pub fn is_valid_primary_key(&self, pk: String) -> bool { self.primary_key(pk).is_some() }
    

    pub fn missing_primary_keys(&self, cols: Vec<String> ) -> Vec<String> {
        
        let mut missing_keys: Vec<String> = Vec::new();
        for pk in &self.primary_keys {
            let pk_name = String::from( pk.get_name() );
            if !cols.contains(&pk_name) { missing_keys.push( pk_name ) }
        }

        missing_keys
    }
}



// |===============================|
// |     Modification function     |
// |===============================|

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        // get the primary keys
        let mut primary_keys: Vec<Column> = Vec::new();
        for c in &columns {
            
            if !c.is_primary_key() { continue; }
    
            let pk = c.clone();
            primary_keys.push(pk);
        }

        // ad a 'tuple id' column if there are no primary keys
        if primary_keys.len() == 0 {
            let id_column = Column::new("Tuple ID".to_string(), DataType::Number, true);
            primary_keys.push(id_column.clone());
            let mut columns = columns.clone();
            columns.push(id_column)
        }

        Self { name: name, columns: columns, primary_keys: primary_keys, rows: Vec::new() }
    }

    
    /// inserts a new row into the database.
    pub fn insert_row(&mut self, row_data: HashMap<String, FieldValue> ) -> Result<(), DBError> {

        // check if the row being inserted is inserting into primary columns
        let keys = row_data.clone().into_keys().collect();
        let missing_primary_keys = self.missing_primary_keys(keys);
        if missing_primary_keys.len() > 0 {
            return Err(DBError::MissingPrimaryKeys( missing_primary_keys ));
        }

        // make sure the primary key isnt already in the db
        for pk in self.primary_keys() {
            let pk_name = pk.get_name();
            let row_pk = row_data.get(pk_name).unwrap();

            for existing_row in &self.rows {
                let existing_row_pk = existing_row.get(pk_name).unwrap();

                if row_pk.eq(existing_row_pk) { 
                    return Err(DBError::DuplicatePrimaryKey(pk_name.to_string()))
                }
            }

        }



        // make sure the FieldValues for each column are correct
        for (col_name, given_field_value) in &row_data {
            let col = self.column(col_name.to_string());

            // check to make sure the column actually exists in the database
            if col.is_none() {
                return Err(DBError::InvalidColumn( String::from(col_name) ))
            }

            let col = col.unwrap();

            // validate datatypes match
            if !col.get_data_type().eq(&given_field_value.data_type()) {
                return Err(DBError::MisMatchDataType(col.get_data_type().clone(), given_field_value.data_type()));
            }
        }

        // if there aren't any missing primary keys, push the hashmap and return unit
        self.rows.push( row_data );
        Ok(())

    }



    pub fn edit_rows(&mut self, column_name: String, search_criteria: FilterCondition, search_value: FieldValue, new_value: FieldValue) -> Result<u32, DBError>{
    
        let filter_result: Result<Table, DBError> = self.select_rows(&column_name, search_criteria, search_value);

        match filter_result { Err(e) => return Err(e), Ok(_) => () };
        let rows_to_edit = filter_result.unwrap();
        let rows_to_edit = rows_to_edit.rows();

        let mut updated_rows: Vec<HashMap<String, FieldValue>> = Vec::new(); 
        for mut row in self.rows().clone() {
            if rows_to_edit.contains( &row ) {
                *row.get_mut(&column_name).unwrap() = new_value.clone();
                updated_rows.push( row );
            } else { updated_rows.push(row);}
        }

        let number_of_changed_rows = rows_to_edit.len() as u32;

        self.rows = updated_rows;

        Ok(number_of_changed_rows)
    }


    /// uses the `Table::filter_rows()` function to determine which rows are to be deleted.
    /// 
    /// returns a u32 of the number of rows deleted if the function does not fail.
    pub fn delete_rows  (&mut self, column_name: String, search_criteria: FilterCondition, search_value: FieldValue ) -> Result<u32, DBError> {

        let filter_result: Result<Table, DBError> = self.select_rows(&column_name, search_criteria, search_value);

        match filter_result { Err(e) => return Err(e), Ok(_) => () };
        let rows_to_delete = filter_result.unwrap();    // safe to unwrap, if it was an err then the line above would early return
        let rows_to_delete = rows_to_delete.rows();

        let mut kept_rows: Vec<HashMap<String, FieldValue>> = Vec::new(); 
        // loop through all rows, if the row is not in `rows_to_delete`, add it to `kept_rows`, which is to then override the existing rows
        for row in self.rows() {
            if rows_to_delete.contains(row) { continue; }   // row is to be deleted

            kept_rows.push( row.clone() );
        }

        // override old row data
        self.rows = kept_rows;

        let number_of_deleted_rows = rows_to_delete.len() as u32; 
        Ok( number_of_deleted_rows )
    }
    
    
    pub fn delete_column(&mut self, column_name: String) -> Result<(), DBError>{
        if !self.is_valid_primary_key(column_name.clone()) {
            return Err(DBError::InvalidColumn(String::from(column_name)))
        }


        // delete the column value from all rows
        for row in &mut self.rows {
            let _ = row.remove_entry(&column_name);
        }

        // remove the column from the column vectors.
        // it doesn't need to be removed from primary_keys vector since an error is thrown
        // at the beginning if the column is a PK
        self.columns.retain(|c| c.get_name() != &column_name);

        Ok(())
    }
    
    
    pub fn index_column(&self, column_name: String) 
    -> Result<BTreeMap<FieldValue, u64>, DBError> {
    
        if self.column(column_name.clone()).is_none() { return Err(DBError::InvalidColumn(column_name.clone())) }


        let mut index: BTreeMap<FieldValue, u64> = BTreeMap::new();

        for (row_index, row) in self.rows().iter().enumerate() {
            let index_key = row.get( &column_name ).unwrap();
            index.insert(index_key.clone(), row_index as u64);              
        }

        Ok(index)
    }
}



// |=======================================|
// |     Sorting / filtering functions     |
// |=======================================|

impl Table {

    pub fn sort_rows(&mut self, sorting_by: SortCondition, column_to_sort_by: String) -> Result<(), DBError> {
        
        if !self.is_valid_column( &column_to_sort_by ) {
            return Err(DBError::InvalidColumn( column_to_sort_by.clone() ));
        }

        match sorting_by {
            SortCondition::NumericAscending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = a.compare_to(b);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
            SortCondition::NumericDescending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = b.compare_to(a);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
            SortCondition::AlphaAscending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = a.compare_to(b);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
            SortCondition::AlphaDescending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = b.compare_to(a);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
            SortCondition::DateAscending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = a.compare_to(b);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
            SortCondition::DateDescending => self.rows.sort_by(|a, b| {
                let a = a.get(&column_to_sort_by).unwrap();
                let b = b.get(&column_to_sort_by).unwrap();
                let comparison_result = b.compare_to(a);
                match comparison_result {
                    Ok(ordering) => ordering,
                    // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                    Err(_) => Ordering::Equal   
                }
            }),
        };


    
        Ok(())
    }


    /// creates a completely new instance of table  with the filtered values
    pub fn select_rows(&mut self, column_name: &String, search_criteria: FilterCondition, value: FieldValue) 
    -> Result< Table, DBError> {

        // check if column actually exists
        if !self.is_valid_column( &column_name ) { 
            return Err(DBError::InvalidColumn(column_name.to_string()))
        }

        let mut matching_rows: Vec<HashMap<String, FieldValue>> = Vec::new();

        // TODO: implement `index_available` and get the index
        matching_rows = if self.index_available(column_name, config::INDEX_PATH) {
            self.search_without_index(column_name, search_criteria, value)?
        } else {
            let index = load_index(config::INDEX_PATH, &self.name, &column_name).unwrap();
            self.search_with_index(index, search_criteria, value)?
        };

        
        let mut filtered_table = Table::new(self.name.clone(), self.columns().clone());

        for r in matching_rows {
            filtered_table.insert_row(r)?
        }

        Ok( filtered_table )
    }


    pub fn index_available(&self, column_name: &str, save_dir: &str) -> bool {
        let table_name = &self.name;
        let path = format!("{save_dir}/{table_name}_{column_name}.bin");
        File::open(path).is_ok()
    }


    fn search_with_index(&self, index: BTreeMap<FieldValue, Vec<u64>>, criteria: FilterCondition, value: FieldValue ) 
    -> Result<Vec<HashMap<String, FieldValue>>, DBError> {

        fn get_from_one_key(rows: Vec<HashMap<String, FieldValue>>, index: BTreeMap<FieldValue, Vec<u64>>, key: FieldValue)
        -> Vec<HashMap<String, FieldValue>> {
            let mut rows: Vec<HashMap<String, FieldValue>> = Vec::new();
             if let Some(row_indices) = index.get(&key) {
                for &row_index in row_indices {
                    if let Some(row) = rows.get(row_index as usize) {
                        rows.push(row.clone());
                    }
                }
            }
            rows
        }

        fn get_all_but_key(rows: Vec<HashMap<String, FieldValue>>, index: BTreeMap<FieldValue, Vec<u64>>, key: FieldValue)
        -> Vec<HashMap<String, FieldValue>> {
            let mut valid_rows: Vec<HashMap<String, FieldValue>> = Vec::new();

            if let Some(row_indices_to_avoid) = index.get(&key) {
                for (idx, row) in rows.iter().enumerate() {
                    if row_indices_to_avoid.contains(&(idx as u64)) { continue; }
                    valid_rows.push( row.clone() );
                }
            }
            valid_rows
        }
 


        let mut matching_rows: Vec<HashMap<String, FieldValue>> = Vec::new();

        // TODO: get values from the index depending on the criteria
        match criteria {
            FilterCondition::LessThan => {
                for (key, row_indices) in index.range(..value) {
                    for &row_index in row_indices {
                        if let Some(row) = self.rows.get(row_index as usize) {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            },
            FilterCondition::LessThanOrEqualTo => {
                for (key, row_indices) in index.range(..=value) {
                    for &row_index in row_indices {
                        if let Some(row) = self.rows.get(row_index as usize) {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            },
            FilterCondition::GreaterThan => {
                for (key, row_indices) in index.range(value..) {
                    for &row_index in row_indices {
                        if let Some(row) = self.rows.get(row_index as usize) {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            },
            FilterCondition::GreaterThanOrEqualTo => {
                 for (key, row_indices) in index.range(value..) {
                    for &row_index in row_indices {
                        if let Some(row) = self.rows.get(row_index as usize) {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            },
            FilterCondition::Equal    => { matching_rows.extend(get_from_one_key(self.rows, index, value)) },
            FilterCondition::NotEqual => { matching_rows.extend( get_all_but_key(self.rows, index, value)) },
            FilterCondition::True     => { matching_rows.extend(get_from_one_key(self.rows, index, FieldValue::Boolean(true)))  },
            FilterCondition::False    => { matching_rows.extend(get_from_one_key(self.rows, index, FieldValue::Boolean(false))) },
            FilterCondition::Null     => { matching_rows.extend(get_from_one_key(self.rows, index, FieldValue::Null)) },
            FilterCondition::NotNull  => { matching_rows.extend( get_all_but_key(self.rows, index, FieldValue::Null)) },
        }

        Err(DBError::ActionNotImplemented("searching for rows with an index".to_string()))
    }


    fn search_without_index(&self, column_name: &String, criteria: FilterCondition, value: FieldValue) 
    -> Result<Vec<HashMap<String, FieldValue>>, DBError> {

        let mut matching_rows: Vec<HashMap<String, FieldValue>> = Vec::new(); 

        // loop through all rows, and if the row matches given criteria, add it to `matching_rows`
        for row in &self.rows {

            // value being checked against the criteria in this specific row
            let target_value = row.get(column_name).unwrap();

            // a copy of the current row to be added to `matching_row` if
            // the row matches the criteria
            let row_copy: HashMap<String, FieldValue> = row.clone();

            // TODO: need to figure out a way to properly implement taking 2 vals for `between`

            // criteria validation
            let row_matches_search_critieria = match criteria {
                // FilterCondition::NumberBetween(l, u)                    => target_value.is_between(l, u),
                // FilterCondition::DateBetween(l, u)  => target_value.date_is_between(l, u),
                FilterCondition::LessThan             => target_value.is_less_than(&value),
                FilterCondition::LessThanOrEqualTo    => target_value.is_leq(&value),
                FilterCondition::GreaterThan          => target_value.is_greater_than(&value),
                FilterCondition::GreaterThanOrEqualTo => target_value.is_geq(&value),
                FilterCondition::Equal                => Ok( target_value.eq(&value)),
                FilterCondition::NotEqual             => Ok(!target_value.eq(&value)),
                FilterCondition::True                 => Ok(target_value.eq( &FieldValue::Boolean(true)  )),
                FilterCondition::False                => Ok(target_value.eq( &FieldValue::Boolean(false) )),
                FilterCondition::Null                 => Ok( target_value.eq(&FieldValue::Null)),
                FilterCondition::NotNull              => Ok(!target_value.eq(&FieldValue::Null)),
            };

            if row_matches_search_critieria.is_err() {
                return Err(row_matches_search_critieria.err().unwrap());
            }

            if row_matches_search_critieria.ok().unwrap() {
                matching_rows.push( row_copy )
            }

        }
        Ok(matching_rows)
    }



    pub fn select_columns(&self, column_names: &Vec<String>) -> Result<Table, DBError> {
        
        let table_name = format!("reduced version of '{}'", &self.name);
        let mut table_columns: Vec<Column> = Vec::new(); 
        
        for c in column_names.clone() {
            if !self.is_valid_column(&c) {
                return Err(DBError::InvalidColumn( c ));
            }

            // find the column given the name
            // yes i know its inefficient, i dont care
            for col in self.columns() {
                if !col.get_name().eq( &c ) { continue; }
            
                table_columns.push(col.clone())
            }

        }
        


        let mut reduced_table = Table::new( table_name, table_columns );
        

        // get new reduced rows
        for current_row in &self.rows {
            let mut reduced_row:HashMap<String, FieldValue> = HashMap::new();
        
            for new_columns in column_names {
                reduced_row.insert(new_columns.to_string(), current_row.get(new_columns).unwrap().clone() );
            }
            let insertion_result = reduced_table.insert_row(reduced_row);

            match insertion_result {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }

        Ok( reduced_table )
    }


}



// |===========================|
// |     display functions     |
// |===========================|
impl Table {
    pub fn to_ascii(&self) -> String {

        let mut text_table = comfy_table::Table::new();

        let mut header_row: Vec<comfy_table::Cell> = Vec::new();
        for col in self.columns() {
            let cell = comfy_table::Cell::new(format!("{}\n({})", col.get_name(), col.get_data_type() ))
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

        format!("\n{}", text_table.to_string())
    }
}


impl Table {
    pub fn save(&self, local_path: String) -> Result<(), DBError> {

        let file_path = format!("{}/db_{}.bin",local_path, self.name);

        let encoded_data = bincode::serialize(&self);
        if encoded_data.is_err() { return Err(DBError::DataBaseFileFailure(file_path.to_owned())) }
        let encoded_data = encoded_data.unwrap();

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



/// loads a database given a filepath. File must be a binary file (extension .bin)
/// 
/// ### Note
/// as of July 2024, the database files are saved in the form "db_{database name}.bin"
/// 
/// ### Examples
/// Valid files:
/// - db_employees.bin
/// - db_wages_2024.bin
/// 
/// Invalid files:
/// - dbEmployees.bin
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


pub fn save_index(save_dir: &str, table_name: &str, column_name: &str, tree: BTreeMap<FieldValue, Vec<u64>>) {

    let file_path: String = format!("{}/idx_{}_{}.bin", save_dir, table_name, column_name);
    let encoded_data = bincode::serialize(&tree).unwrap();

    let mut file = File::create(file_path).unwrap();
    file.write_all(&encoded_data).unwrap();
}


pub fn load_index(save_dir: &str, table_name: &str, column_name: &str) -> Option<BTreeMap<FieldValue, Vec<u64>>> {
    let file_path: String = format!("{}/idx_{}_{}.bin", save_dir, table_name, column_name);
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