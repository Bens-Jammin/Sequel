use std::{
    cmp::Ordering, 
    collections::{BTreeMap, HashMap}, 
    fs::{File, OpenOptions}, 
    io::{Read, Write}
};
use chrono::DateTime;
use comfy_table::presets::ASCII_MARKDOWN;
use serde::{Deserialize, Serialize};
use bincode;
use crate::{
    config::{self, INDEX_PATH}, 
    structures::{
        column::{Column, DataType, FieldValue}, 
        db_err::DBError, filter::{FilterCondition, FilterConditionValue}, 
        sort::SortCondition
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
/// * (TOP PRIOTIRY) read 13.2.1 in DB textbook, learn to implement records ( ALSO STUDY FOR MIDTERM(S) )
/// * (TOP PRIORITY) implement index for primary keys, update them on insertion
/// * (LOW PRIORITY) learn how to cache values (such as the index and relation paths)
/// =====================================================================================


// |===========================|
// |     utility functions     |
// |===========================|

impl Table {
    pub fn rows(&self) -> &Vec<HashMap<String, FieldValue>> { &self.rows }

    pub fn number_of_rows(&self) -> usize { self.rows.len() }

    pub fn get_row(&self, row_index: usize) -> Option<&HashMap<String, FieldValue>> { self.rows.get(row_index) }

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


/// converts a string into its file name counterpart. Used to help find a file for a possible relation
pub fn format_for_file_name(str: &str) -> String {
    str.to_uppercase().replace(" ", "_")
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

    
    /// inserts a new row into the database.
    pub fn insert_row(&mut self, row_data: &HashMap<String, FieldValue> ) -> Result<(), DBError> {

        // check if the row being inserted is inserting into primary columns
        let keys = row_data.clone().into_keys().collect();
        let missing_primary_keys = self.missing_primary_keys(keys);
        if missing_primary_keys.len() > 0 {
            return Err(DBError::MissingPrimaryKeys( missing_primary_keys ));
        }

        // TODO: implement the same logic using the BTreeMap
        

        // make sure the primary key isnt already in the db
        // for pk in self.primary_keys() {
        //     let pk_name = pk.get_name();
        //     let row_pk = row_data.get(pk_name).unwrap();

        //     for existing_row in &self.rows {
        //         let existing_row_pk = existing_row.get(pk_name).unwrap();

        //         if row_pk.eq(existing_row_pk) { 
        //             return Err(DBError::DuplicatePrimaryKey(pk_name.to_string()))
        //         }
        //     }
        // }



        // make sure the FieldValues for each column are correct
        for (col_name, given_field_value) in row_data {
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
        self.rows.push( row_data.clone() );
        Ok(())

    }


    // NOTE: the update functions aren't in the table code, I have to add it 
    // TODO: implement `update_index_modify`
    #[allow(dead_code)]
    fn update_index_modify(&self, column_name: &str ) -> Result<(), DBError> {
        
        let read_result = load_index(INDEX_PATH, &self.name, column_name);
        if read_result.is_none() {
            return Err(DBError::IOFailure( index_file_name(&self.name, column_name) , "failed to load index from file.".to_owned() ));
        } // else {  
        let index = read_result.unwrap();
        // }

        Err(DBError::ActionNotImplemented( "Index updating - modify".to_string()))
    } 


    // TODO: test this
    #[allow(dead_code)]
    fn update_index_deletion(&self, column_name: &str, fv_from_deleted_row: &FieldValue ) -> Result<(), DBError> {

        let read_result = load_index(INDEX_PATH, &self.name, column_name);
        if read_result.is_none() {
            return Err(DBError::IOFailure( index_file_name(&self.name, column_name) , "failed to load index from file.".to_owned() ));
        } // else {  
        let index = read_result.unwrap();
        // }

        Err(DBError::ActionNotImplemented("Index Updating - deletion".to_string()))
        // Ok(())
    }


    // TODO: test this
    #[allow(dead_code)]
    fn update_index_insertion(&self, column_name: &str, fv_from_inserted_row: &FieldValue, row_index: usize) -> Result<(), DBError> {

        let read_result = load_index(INDEX_PATH, &self.name, column_name);
        if read_result.is_none() {
            return Err(DBError::IOFailure( index_file_name(&self.name, column_name) , "failed to load index from file.".to_owned() ));
        } // else {  
        let index = read_result.unwrap();
        // }

        Err(DBError::ActionNotImplemented("Index Updating - insertion".to_string()))
        // Ok(())

    } 


    pub fn edit_rows(
        &mut self, 
        filter_column_name: String,
        column_to_edit: String, 
        search_criteria: FilterCondition, 
        new_value: FieldValue
    ) -> Result<u32, DBError>{
    
        let filter_result: Result<Table, DBError> = self.select_rows(&filter_column_name, search_criteria);

        match filter_result { Err(e) => return Err(e), Ok(_) => () };
        let rows_to_edit = filter_result.unwrap();
        let rows_to_edit = rows_to_edit.rows();

        let mut updated_rows: Vec<HashMap<String, FieldValue>> = Vec::new(); 
        for mut row in self.rows().clone() {
            if rows_to_edit.contains( &row ) {
                *row.get_mut(&column_to_edit).unwrap() = new_value.clone();
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


        let mut index: BTreeMap<FieldValue, Vec<usize>> = BTreeMap::new();

        // Iterate over each row and build the index
        for (row_index, row) in self.rows().iter().enumerate() {
            // Get the value of the specified column in the current row
            if let Some(index_key) = row.get(&column_name) {
                // Check if the key is already in the index
                // If it exists, push the row index to the vector, 
                // otherwise insert a new vector with the row index
                index.entry(index_key.clone())
                    .and_modify(|v| v.push(row_index)) // Add to existing vector if key exists
                    .or_insert_with(|| vec![row_index]); // Insert new vector with the current row index
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



    /// creates a completely new instance of table  with the filtered values
    pub fn select_rows(&mut self, column_name: &String, search_criteria: FilterCondition) -> Result<Table, DBError> {

        // check if column actually exists
        if !self.is_valid_column( &column_name ) { 
            return Err(DBError::InvalidColumn(column_name.to_string()))
        }


        let matching_rows = if self.index_available(column_name, config::INDEX_PATH) {
            let index = load_index(config::INDEX_PATH, &self.name, &column_name).unwrap();
            // O(n^0.67)
            self.search_with_index(index, search_criteria)?
        } else {
            // O(n^1.8) 
            self.search_without_index(column_name, search_criteria)?
        };

        let mut filtered_table = Table::new(self.name.clone(), self.columns().clone());

        for r in matching_rows {
            filtered_table.insert_row( r )?
        }

        Ok( filtered_table )
    }


    pub fn index_available(&self, column_name: &str, save_dir: &str) -> bool {
        let path = format!("{save_dir}/{}", index_file_name(&self.name, column_name) );
        File::open(path).is_ok()
    }


    fn search_with_index(&self, index: BTreeMap<FieldValue, Vec<usize>>, criteria: FilterCondition) 
    -> Result<Vec<&HashMap<String, FieldValue>>, DBError> {

        fn find_row_indices(index: BTreeMap<FieldValue, Vec<usize>>, range: impl std::ops::RangeBounds<FieldValue>) -> Vec<usize>{
            index.range(range)
                .flat_map(|(_, v)| v.iter().map(|idx| *idx))
                .collect::<Vec<usize>>()
        }

        /// makes sure that, to ensure the range is properly built for the index 
        fn validate_condition_is_number(condition: &FilterConditionValue ) -> Result<(), DBError> {
            if condition.number().is_none() {
                return Err(DBError::MisMatchConditionDataType(
                    FilterConditionValue::Number(-1.0),
                    condition.clone()
                ))
            }
            return Ok(())
        }
    

        fn validate_condition_is_number_range(condition: &FilterConditionValue ) -> Result<(), DBError> {
            if condition.number_range().is_none() {
                return Err(DBError::MisMatchConditionDataType(
                    FilterConditionValue::Number(-1.0),
                    condition.clone()
                ))
            }
            return Ok(())
        }


        fn search_index_for_bool_or_null(index: BTreeMap<FieldValue, Vec<usize>>, fv: &FieldValue) -> Vec<usize> {
            match index.get(fv) {
                Some(indices) => indices.clone(),
                None => Vec::new(),
            }
        }


        let eligible_row_indices: Vec<usize> = match criteria {
            FilterCondition::LessThan(condition_value) => {
                validate_condition_is_number(&condition_value)?;
                let search_value = FieldValue::Number(condition_value.number().unwrap());
                find_row_indices(index, ..search_value)
            },
            FilterCondition::LessThanOrEqualTo(condition_value) => {
                validate_condition_is_number(&condition_value)?;
                let search_value = FieldValue::Number(condition_value.number().unwrap());
                find_row_indices(index, ..=search_value)
            },
            FilterCondition::GreaterThan(condition_value) => {
                validate_condition_is_number(&condition_value)?;
                let search_value = FieldValue::Number(condition_value.number().unwrap() + 0.00000001);
                find_row_indices(index, search_value..)
            },
            FilterCondition::GreaterThanOrEqualTo(condition_value) => {
                validate_condition_is_number(&condition_value)?;
                let search_value = FieldValue::Number(condition_value.number().unwrap());
                find_row_indices(index, search_value..)
            },
            FilterCondition::Equal(condition_value) => {
                if condition_value.number().is_none() { 
                    return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(-1.0), condition_value));
                }
                let search_value = FieldValue::Number(condition_value.number().unwrap());
                match index.get(&search_value) {
                    Some(indices) => indices.clone(),
                    None => return Ok(Vec::new()),
                }
            },
            FilterCondition::NumberBetween(condition_value) => {
                validate_condition_is_number_range(&condition_value)?;

                let (lower_bound, upper_bound) = condition_value.number_range().unwrap();
                let lower_bound = FieldValue::Number(lower_bound);
                let upper_bound = FieldValue::Number(upper_bound);
                find_row_indices(index, lower_bound..=upper_bound)
            },
            FilterCondition::DateBetween(condition_value) => {
                validate_condition_is_number_range(&condition_value)?;                

                let (lower_bound, upper_bound) = condition_value.date_range().unwrap();
                let lower_bound = FieldValue::Date(lower_bound);
                let upper_bound = FieldValue::Date(upper_bound);
                find_row_indices(index, upper_bound..=lower_bound)
            },
            FilterCondition::NotEqual(_) => return Err(DBError::ActionNotImplemented("Indexing on inequality".to_owned())),
            FilterCondition::NotNull     => return Err(DBError::ActionNotImplemented("Indexing on non-null values".to_owned())),
            FilterCondition::True  => search_index_for_bool_or_null(index, &FieldValue::Boolean(true)  ),
            FilterCondition::False => search_index_for_bool_or_null(index, &FieldValue::Boolean(false) ),
            FilterCondition::Null  => search_index_for_bool_or_null(index, &FieldValue::Null           ),
        };

        let mut rows: Vec<&HashMap<String, FieldValue>> = Vec::with_capacity( eligible_row_indices.len() );
        let table_rows = self.rows();
        for row_idx in eligible_row_indices {
            rows.push( &table_rows[row_idx] );
        }

        Ok(rows)
    }


    fn search_without_index(&self, column_name: &String, criteria: FilterCondition) 
    -> Result<Vec<&HashMap<String, FieldValue>>, DBError> {

        let mut matching_rows: Vec<&HashMap<String, FieldValue>> = Vec::new(); 

        for row in &self.rows {
            let row_value: &FieldValue = row.get(column_name).unwrap();

            if non_index_row_matches_search_critieria(&row_value, &criteria)? {
                matching_rows.push( row )
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
            let insertion_result = reduced_table.insert_row(&reduced_row);

            match insertion_result {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }

        Ok( reduced_table )
    }


}


fn non_index_row_matches_search_critieria(row_value: &FieldValue, search_criteria: &FilterCondition) 
-> Result<bool, DBError> {
    match &search_criteria {
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
                    Ok(FieldValue::Number(*lower_bound).is_less_than(row_value)? 
                    && FieldValue::Number(*upper_bound).is_greater_than(row_value)?)
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
                    Ok(FieldValue::Date(*lower_bound).is_less_than(row_value)? 
                    && FieldValue::Date(*upper_bound).is_greater_than(row_value)?)
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
            let cell = comfy_table::Cell::new(format!("{}\n<{}>", col.get_name(), col.get_data_type() ))
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

        let file_path = format!("{}/{}",local_path, relation_file_name( &self.to_file_name() ));
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
    pub fn to_excel(&self) -> Result<(), DBError> { todo!(); }
    pub fn to_csv(&self)   -> Result<(), DBError> { 
        let file_path = format!("{}/{}", config::EXPORT_PATH, self.file_name_for_export("csv") );

        let file_creation_result = File::create(&file_path);
        if file_creation_result.is_err() {
            return Err(DBError::IOFailure(file_path, "Failed to create file".to_string()));
        }

        let mut _file = file_creation_result.unwrap(); 

        // TODO: for CSV writing 
        // 1. iterate over columns, write their names
        // 2. iterate over rows, write their data
        // 3. write to file, throw a "writing to file" error if one occurs. 

        Ok(())
     }


    fn file_name_for_export(&self, file_extension: &str) -> String {
        format!("sequelDB_{}.{}", &self.name, file_extension)
    } 
}


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


pub fn save_index(save_dir: &str, table_name: &str, column_name: &str, tree: BTreeMap<FieldValue, Vec<usize>>) {

    let file_path: String = format!("{}/{}",save_dir, index_file_name(table_name, column_name));
    let encoded_data = bincode::serialize(&tree).unwrap();
    let mut file = File::create(file_path).unwrap();
    file.write_all(&encoded_data).unwrap();
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


pub fn index_file_name(table_name: &str, column_name: &str) -> String {
    format!("idx_{}_{}.bin", table_name, column_name)
}


pub fn relation_file_name(name: &String) -> String {
    format!("db_{}.bin", name )
}