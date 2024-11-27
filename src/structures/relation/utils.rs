use std::collections::{BTreeMap, HashMap};

use crate::{config::INDEX_PATH, structures::{column::{Column, FieldValue}, db_err::DBError}};

use super::{io::{index_file_name, load_index}, table::Table};




impl Table {

    pub fn name(&self) -> String {
        self.name.clone()
    }
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

}


impl Table {
    /// # WARNING
    /// This is a TEMPORARY FUNCTION USED FOR TESTING PURPOSES ONLY ! <br>
    /// if you are seeing this outside of the sequel source code, something has gone seriously wrong, contact `bmill079@uottawa.ca` ASAP.
    pub fn index_on(&self, column_name: &str) -> Result<BTreeMap<FieldValue, Vec<usize>>, DBError> {
        match load_index(INDEX_PATH, &self.name, column_name) {
            Some(i) => Ok(i),
            None => Err(DBError::IOFailure( index_file_name(&self.name, column_name) , "failed to load index from file.".to_owned() ))
        }
    }
}