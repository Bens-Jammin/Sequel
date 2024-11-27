use std::collections::{BTreeMap, HashMap};

use crate::{config::INDEX_PATH, structures::{column::{Column, DataType, FieldValue}, db_err::DBError, filter::FilterCondition}};

use super::{io::{load_index, save_index}, table::Table};


impl Table {
     // TODO: implement Aggregate functions
    pub fn new(name: String, columns: Vec<Column>, disable_primary_keys: bool) -> Self {
        // get the primary keys
        let mut primary_keys: Vec<Column> = Vec::new();
        
        if !disable_primary_keys {


            for c in &columns {
                
                if !c.is_primary_key() { continue; }
        
                let pk = c.clone();
                primary_keys.push(pk);
            }
        
        }

        // add a 'tuple id' column if there are no primary keys
        // ONLY IF primary keys are enabled
        if !disable_primary_keys && primary_keys.len() == 0 {
            let id_column = Column::new("Tuple ID".to_string(), DataType::Number, true);
            primary_keys.push(id_column.clone());
            let mut columns = columns.clone();
            columns.push(id_column);
        }

        let instance = Self { name, columns, primary_keys: primary_keys.clone(), rows: Vec::new() };

        // generate indexes on all primary keys
        for pk in &primary_keys {
            let _ = instance.index_column(pk.get_name().to_owned());
        }

        instance
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


    /// inserts a new row into the database.
    pub fn insert_row(&mut self, row_data: &HashMap<String, FieldValue> ) -> Result<(), DBError> {

        // check if the row being inserted is inserting into primary columns
        let keys = row_data.clone().into_keys().collect();
        let missing_primary_keys = self.missing_primary_keys(keys);
        if missing_primary_keys.len() > 0 {
            return Err(DBError::MissingPrimaryKeys( missing_primary_keys ));
        }
        

        // make sure the primary key isnt already in the db
        for pk in self.primary_keys() {
            let pk_name = pk.get_name();
            let new_row_field_value_at_pk = row_data.get(pk_name).unwrap();
            let pk_index = load_index( INDEX_PATH, &self.name, pk_name ).unwrap();

            if pk_index.contains_key( new_row_field_value_at_pk ) {
                println!("already have {}", new_row_field_value_at_pk);
                return Err(DBError::DuplicatePrimaryKey(pk_name.to_string()))
            }
        }



        // make sure the FieldValues for each column are correct
        for (col_name, given_field_value) in row_data {
            let col = self.column(col_name.to_string());

            // check to make sure the column actually exists in the database
            if col.is_none() {
                return Err(DBError::InvalidColumn( String::from(col_name) ))
            }

            let col = col.unwrap();

            // validate datatypes match
            if !given_field_value.eq(&FieldValue::Null) && !col.get_data_type().eq(&given_field_value.data_type()) {
                return Err(DBError::MisMatchDataType(col.get_data_type().clone(), given_field_value.data_type()));
            }
        }

        // if there aren't any missing primary keys, push the hashmap and return unit
        self.rows.push( row_data.clone() );

        for indexed_column in self.primary_keys() {
            let column_name = indexed_column.get_name();

            self.update_index_insertion( 
                &column_name, 
                row_data.get(column_name).unwrap(), 
                self.rows.len() - 1 
            )?;
        }

        Ok(())

    }



    fn update_index_insertion(&self, column_name: &str, fv_from_inserted_row: &FieldValue, row_index: usize) -> Result<(), DBError> {

        let mut index = self.index_on(column_name)?;

        index.insert( fv_from_inserted_row.clone() , vec![row_index] );

        save_index( INDEX_PATH, &self.name, column_name, index );
        Ok(())

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

        /* 
        in order to update the indexes for this table, we need the following information:
        1. all the indexes available for this table
        2. all the field values for all rows being updated, for all the columns
        3. the field value which is replacing the outdated values
        
        heres pseudocode of my algorithm:
        for all of the indexes (which iterates over a vector of referenced columns):
            load the index into memory
            for all of the rows being updated:
                load the field value from that row and column (grabbed from the outer for loop)
                delete that field value from the index, which will return the row indices being stored there
                
                if the field value doesn't already exist in the index (i.e. this is after the first iteration): 
                    insert the new field value with the row index from the previously deleted field value into the index
                otherwise:
                    get the vector of indices being stored at that fieldvalue in the index
                    concatenate the recently retrieved indices to that vector
                    override the existing index value with the newly concatenated vector of row indices
            save the index
        */ 

        for indexed_column in self.primary_keys() {
            let indexed_column_name = indexed_column.get_name();
            let mut index = self.index_on(indexed_column_name)?;

            for row in rows_to_edit {
                let old_field_value = row.get(indexed_column_name).unwrap();
                index.remove( old_field_value );

                
                let row_index = self.rows().iter().position(|r| r == row).unwrap();
                if index.contains_key( &new_value ) {
                    let mut existing_row_indices = index.remove( &new_value ).unwrap();
                    existing_row_indices.push(row_index);
                    index.insert( new_value.clone() , existing_row_indices );

                } else {
                    index.insert(new_value.clone(), vec![row_index] );
                }
            }
            save_index(INDEX_PATH, &self.name, indexed_column_name, index);
        }



        // I honestly have no idea how this works but whatever, have fun debugging this later dipshit
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

        let temp_index = load_index(INDEX_PATH, &self.name, "A" ).unwrap();
        println!(" wayy before deleting data in index on {}: ", "A");
        for (k, v) in &temp_index {
            println!("fv: {} | row idx: {:?}", k, v);
        }
        println!("=== END OF INDEX ===\n\n");

        let filtered_table = self.select_rows(&column_name, search_criteria)?;
        let rows_to_delete = filtered_table.rows();
        
        let kept_rows: Vec<HashMap<String, FieldValue>> = self
            .rows()
            .iter()
            .filter(|r| !rows_to_delete.contains(*r) )
            .cloned()
            .collect();
        
        
        // iterate through the indexed columns, deleting the values from any rows that have been removed
        for indexed_column in self.primary_keys() {
            let mut index = load_index(INDEX_PATH, &self.name, indexed_column.get_name() ).unwrap();
            
            for row in rows_to_delete {

                let column_name = indexed_column.get_name();
    
                    index.remove(row.get(column_name).unwrap());
            }
            
           save_index( INDEX_PATH, &self.name, indexed_column.get_name(), index );
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
}