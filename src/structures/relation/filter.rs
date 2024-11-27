
use std::{collections::{BTreeMap, HashMap}, fs::File};

use crate::{config::INDEX_PATH, structures::{column::{Column, FieldValue}, db_err::DBError, filter::{FilterCondition, FilterConditionValue}}};

use super::{io::{index_file_name, load_index}, search::non_index_row_matches_search_critieria, table::Table};


impl Table {

    /// creates a completely new instance of table  with the filtered values
    pub fn select_rows(&mut self, column_name: &String, search_criteria: FilterCondition) -> Result<Table, DBError> {

        // check if column actually exists
        if !self.is_valid_column( &column_name ) { 
            return Err(DBError::InvalidColumn(column_name.to_string()))
        }


        let matching_rows = if self.index_available(column_name, INDEX_PATH) {
            let index = load_index(INDEX_PATH, &self.name, &column_name).unwrap();
            // O(n^0.67)
            self.search_with_index(index, search_criteria)?
        } else {
            // O(n^1.8) 
            self.search_without_index(column_name, search_criteria)?
        };

        // a new name is required because this table would override the actual table, incluidng index data 
        let mut filtered_table = Table::new(format!("temp table {} with filtered rows on column {}",&self.name, column_name), self.columns().clone(), true);

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
    

        let mut reduced_table = Table::new( format!("{} with filtered columns", table_name), table_columns, true );
        

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