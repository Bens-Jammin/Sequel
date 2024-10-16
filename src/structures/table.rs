use std::{cmp::Ordering, collections::{BTreeMap, HashMap}, fs::{File, OpenOptions}, io::{Read, Write}, usize};
use chrono::DateTime;
use comfy_table::presets::ASCII_MARKDOWN;
use serde::{Deserialize, Serialize};
use bincode;
use crate::{
    config::{self, INDEX_PATH}, 
    structures::{
        column::{Column, DataType, FieldValue}, 
        db_err::DBError, modify_where::{FilterCondition, FilterConditionValue}, 
        sort_method::SortCondition
    }
};


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    primary_keys: Vec<Column>,
    rows: Vec<HashMap<String, FieldValue>>
}

/// ====================================================================================
/// TODO: 
/// * (TOP PRIORITY) learn how to cache values (such as the index and relation paths)
/// * (TOP PRIORITY) update b+ trees on update
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
    

    /// given a list of columns to be inserted, returns a list of primary key columns which are missing from the list
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

        let instance = Self { name: name, columns: columns, primary_keys: primary_keys.clone(), rows: Vec::new() };

        // generate indexes on all primary keys
        for pk in &primary_keys {
            let _ = instance.index_column(pk.get_name().to_owned());
        }

        instance
    }

    
    fn update_index(&self, column_name: &str) -> Result<(), DBError> {
        Err(DBError::ActionNotImplemented("Index Updating".to_string()))
    } 

    /// inserts a new row into the database.
    pub fn insert_row(&mut self, row_data: HashMap<String, FieldValue> ) -> Result<(), DBError> {

        // check if the row being inserted is inserting into primary columns
        let keys = row_data.clone().into_keys().collect();
        let missing_primary_keys = self.missing_primary_keys(keys);
        if missing_primary_keys.len() > 0 {
            return Err(DBError::MissingPrimaryKeys( missing_primary_keys ));
        }

        // TODO: inserting **ONE ROW** takes O(n) !! FIX ASAP
        // implement a B+ tree to help fix it
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



    pub fn edit_rows(&mut self, column_name: String, search_criteria: FilterCondition, new_value: FieldValue) -> Result<u32, DBError>{
    
        let filter_result: Result<Table, DBError> = self.select_rows(&column_name, search_criteria);

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
    pub fn delete_rows(&mut self, column_name: String, search_criteria: FilterCondition ) -> Result<u32, DBError> {

        let filter_result: Result<Table, DBError> = self.select_rows(&column_name, search_criteria);

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
    
    /// makes an index on `column_name` and automatically saves it to the config directory
    pub fn index_column(&self, column_name: String) -> Result<(), DBError> {
        
        if self.column(column_name.clone()).is_none() { return Err(DBError::InvalidColumn(column_name.clone())) }


        let mut index: BTreeMap<FieldValue, Vec<u64>> = BTreeMap::new();

        // Iterate over each row and build the index
        for (row_index, row) in self.rows().iter().enumerate() {
            // Get the value of the specified column in the current row
            if let Some(index_key) = row.get(&column_name) {
                // Check if the key is already in the index
                // If it exists, push the row index to the vector, otherwise insert a new vector with the row index
                index.entry(index_key.clone())
                    .and_modify(|v| v.push(row_index as u64)) // Add to existing vector if key exists
                    .or_insert_with(|| vec![row_index as u64]); // Insert new vector with the current row index
            }

        }

        save_index(INDEX_PATH, &self.name, &column_name, index);

        Ok(())
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

    /// # NOTE: 
    /// used for testing index speed ONLY!!
    pub fn scan(&mut self, column_name: &String, search_criteria: FilterCondition)
    -> Result<Table, DBError> {

        // check if column actually exists
        if !self.is_valid_column( &column_name ) { 
            return Err(DBError::InvalidColumn(column_name.to_string()))
        }


        let matching_rows = if false {
            let index = load_index(config::INDEX_PATH, &self.name, &column_name).unwrap();
            self.search_with_index(index, search_criteria)?
        } else {
            self.search_without_index(column_name, search_criteria)?
        };

        
        let mut filtered_table = Table::new(self.name.clone(), self.columns().clone());

        for r in matching_rows {
            filtered_table.insert_row(r)?
        }

        Ok( filtered_table )
    }


    /// creates a completely new instance of table  with the filtered values
    pub fn select_rows(&mut self, column_name: &String, search_criteria: FilterCondition) -> Result<Table, DBError> {

        // check if column actually exists
        if !self.is_valid_column( &column_name ) { 
            return Err(DBError::InvalidColumn(column_name.to_string()))
        }


        let matching_rows = if false /* self.index_available(column_name, config::INDEX_PATH) */ {
            let index = load_index(config::INDEX_PATH, &self.name, &column_name).unwrap();
            self.search_with_index(index, search_criteria)?
        } else {
            self.search_without_index(column_name, search_criteria)?
        };

        let mut filtered_table = Table::new(self.name.clone(), self.columns().clone());

        for r in matching_rows {
            filtered_table.insert_row(r)?
        }

        Ok( filtered_table )
    }


    pub fn index_available(&self, column_name: &str, save_dir: &str) -> bool {
        let path = format!("{save_dir}/{}", index_file_name(&self.name, column_name) );
        File::open(path).is_ok()
    }


    fn search_with_index(&self, index: BTreeMap<FieldValue, Vec<u64>>, criteria: FilterCondition) 
    -> Result<Vec<HashMap<String, FieldValue>>, DBError> {
        Err( DBError::ActionNotImplemented("searching the relation with an index".to_string()))
    }


    fn search_without_index(&self, column_name: &String, criteria: FilterCondition) 
    -> Result<Vec<HashMap<String, FieldValue>>, DBError> {

        let mut matching_rows: Vec<HashMap<String, FieldValue>> = Vec::new(); 

        // loop through all rows, and if the row matches given criteria, add it to `matching_rows`
        for row in &self.rows {
            let row_value = row.get(column_name).unwrap();

            // a copy of the current row to be added to `matching_row` if
            // the row matches the criteria
            let row_copy: HashMap<String, FieldValue> = row.clone();

            // criteria validation
            let row_matches_search_critieria = match &  criteria {
                // check if the condition is a relational operator (i.e. >, >=, ==, !=, <, <=)
                FilterCondition::LessThan(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            row_value.is_less_than(&FieldValue::Number(*target_value))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::LessThanOrEqualTo(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            row_value.is_leq(&FieldValue::Number(*target_value))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::GreaterThan(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            row_value.is_greater_than(&FieldValue::Number(*target_value))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::GreaterThanOrEqualTo(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            row_value.is_geq(&FieldValue::Number(*target_value))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::Equal(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            Ok(row_value.eq(&FieldValue::Number(*target_value)))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::NotEqual(condition_value) => {
                    match &condition_value {
                        FilterConditionValue::Number(target_value) => {
                            Ok(!row_value.eq(&FieldValue::Number(*target_value)))
                        },
                        _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
                }},
                FilterCondition::NumberBetween(condition_value) => {
                    // make sure the target value is a range so we can see if the cell value is in a range
                    match &condition_value { 
                        FilterConditionValue::NumberRange(lower_bound, upper_bound) => {
                            Ok(FieldValue::Number(*lower_bound).is_less_than(row_value)? && FieldValue::Number(*upper_bound).is_greater_than(row_value)?)
                        },
                         _ => return Err(DBError::MisMatchConditionDataType(
                            FilterConditionValue::DateRange(DateTime::default(), DateTime::default()), condition_value.clone()
                        )) 
                    }
                },
                FilterCondition::DateBetween(condition_value) => {
                    // make sure the target value is a range so we can see if the cell value is in a range
                    match &condition_value { 
                        FilterConditionValue::DateRange(lower_bound, upper_bound) => {
                            Ok(FieldValue::Date(*lower_bound).is_less_than(row_value)? && FieldValue::Date(*upper_bound).is_greater_than(row_value)?)
                        },
                         _ => return Err(DBError::MisMatchConditionDataType(
                            FilterConditionValue::NumberRange(0.0, 0.0), condition_value.clone()
                        )) 
                    }
                },
                FilterCondition::True                 => Ok( row_value.eq( &FieldValue::Boolean(true)  )),
                FilterCondition::False                => Ok( row_value.eq( &FieldValue::Boolean(false) )),
                FilterCondition::Null                 => Ok( row_value.eq(&FieldValue::Null)),
                FilterCondition::NotNull              => Ok(!row_value.eq(&FieldValue::Null)),
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

        text_table.load_preset(ASCII_MARKDOWN).remove_style(comfy_table::TableComponent::HorizontalLines);
        
        format!("\n{}", text_table.to_string())
    }
}


impl Table {
    pub fn save(&self, local_path: String) -> Result<(), DBError> {

        let file_path = format!("{}/{}",local_path, relation_file_name(&self.name));

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

    // TODO:
    pub fn to_excel(&self) -> Result<(), DBError> {todo!(); }
    pub fn to_csv(&self)   -> Result<(), DBError> { todo!(); }
}
// TODO: save / load files using capitalized names
// TODO: make a function that turns the table into its saved name
// TODO: make a function that given a string, returns a **POSSIBLE** table name (i.e. enter "test_db", return "TEST_DB" because of the above function) 


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

    let file_path: String = format!("{}/{}",save_dir, index_file_name(table_name, column_name));
    let encoded_data = bincode::serialize(&tree).unwrap();
    let mut file = File::create(file_path).unwrap();
    file.write_all(&encoded_data).unwrap();
}


pub fn load_index(save_dir: &str, table_name: &str, column_name: &str) -> Option<BTreeMap<FieldValue, Vec<u64>>> {
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


pub fn index_file_name(table_name: &str, column_name: &str) -> String {
    format!("idx_{}_{}.bin", table_name, column_name)
}


pub fn relation_file_name(table_name: &str) -> String {
    format!("db_{}.bin", table_name)
}