use std::{cmp::Ordering, collections::HashMap};

use crate::structures::{column::{Column, FieldValue}, db_err::DBError};
use super::table::Table;


impl Table {

   
    // TODO: implement logging and recovery (ARIES if possible, simplified recovery function otherwise)
    // TODO: implement Aggregation ? maybe?
    // MIN, MAX, AVG, MED(ian), Count, SUM, GROUP (by) 
    
    /// performs a cartesian product on the two tables
    pub fn cartesian_join(&self, other: &Table) -> Result<Table, DBError> {

        let mut join_table_columns: Vec<Column> = Vec::new();

        for col in self.columns() {
            let mut c = col.clone();
            c.change_pk_state( false );
            join_table_columns.push( c );
        }
        for col in other.columns() {
                let mut c = col.clone();
                c.new_name( format!("{} (S)", c.get_name()) );
                c.change_pk_state( false );
                join_table_columns.push( c );
        }        
        let mut join_table: Table = Table::new(
            format!("Cartesian Join Result of Tables {} and {}", self.name(), other.name()),
            join_table_columns,
            true
        );
        
        // Nested loop join method
        for r_row in self.rows() {
            for s_row in other.rows() {
                let mut joined_row = r_row.clone();

                for (k, v) in s_row {
                    // don't forget to reformat the column names for the rows in `other` !
                    joined_row.insert( format!("{} (S)", k), v.clone());
                }
                join_table.insert_row(&joined_row)?;
            }
        }

        Ok(join_table)
    }


    pub fn outer_join(&self, other: &Table, column_to_join: String) -> Result<Table, DBError> {
        #[derive(Debug)]
        struct JoinPair { value_to_sort_on: FieldValue, row_index: usize }

        fn cmp_pairs(p1: &JoinPair, p2: &JoinPair) -> Ordering {
            p1.value_to_sort_on.cmp(&p2.value_to_sort_on)
        }
        fn join_rows(r1: &HashMap<String, FieldValue>, r2: &HashMap<String, FieldValue>, join_column: &String) -> HashMap<String, FieldValue> {
            let mut result = HashMap::new();
            for (k, v) in r1 {
                result.insert(k.to_string(), v.clone());
            }
            for (k, v) in r2 {
                if k == join_column { continue; }
                result.insert(k.to_string(), v.clone());
            }
    
            result
        }

        let mut join_table_columns: Vec<Column> = Vec::new();

        for col in self.columns() {
            if col.get_name() == column_to_join { continue; }
            let mut c = col.clone();
            c.change_pk_state( false );
            join_table_columns.push( c );
        }
        for col in other.columns() {
            let mut c = col.clone();
            c.change_pk_state( false );
            join_table_columns.push( c );
        }

        let mut join_table: Table = Table::new(
            format!("Join Result of Tables {} and {} on column {}", self.name(), other.name(), &column_to_join),
            join_table_columns,
            true
        );


        // make sure there's at least one element
        if self.rows().len() == 0 || other.rows().len() == 0 {
            return Ok(join_table)
        }

        let mut r_join_elements: Vec<JoinPair> = Vec::new();
        let mut s_join_elements: Vec<JoinPair> = Vec::new();

        for (idx, r) in self.rows().iter().enumerate() {
            let field_value = r.get(&column_to_join).unwrap();
            r_join_elements.push( JoinPair{ value_to_sort_on: field_value.clone(), row_index: idx} );
        } 
        for (idx, r) in other.rows().iter().enumerate() {
            let field_value = r.get(&column_to_join).unwrap();
            s_join_elements.push( JoinPair{ value_to_sort_on: field_value.clone(), row_index: idx} );
        }

        
        r_join_elements.sort_by(|a, b| cmp_pairs(a, b) );
        s_join_elements.sort_by(|a, b| cmp_pairs(a, b) );
        


        let mut marked_row: Option<usize> = None;
        let mut r_pointer: usize = 0;
        let mut s_pointer: usize = 0;
        let mut r_ptr_in_result: bool = false;
        let mut skipped_rows: Vec<usize> = Vec::new();

        // TODO: need to rethink the whole skipped row vector thing

        'outer: loop {
            // stop when one list ran out of elements
            if r_pointer == r_join_elements.len() || s_pointer == s_join_elements.len() {
                break 'outer;
            }

            if marked_row.is_none() {

                'until_eq: loop {
                    let row_cmp_result = cmp_pairs(&r_join_elements[r_pointer], &s_join_elements[s_pointer]);
                    if row_cmp_result == Ordering::Equal     { break 'until_eq; }
                    else if row_cmp_result == Ordering::Less { 
                        // if the current row in r isn't in the join result, it was skipped
                        if !r_ptr_in_result { skipped_rows.push( (&r_join_elements[r_pointer]).row_index ); }
                        r_ptr_in_result = false; 
                        r_pointer += 1;
                    }
                    else /* if r > s */ { s_pointer += 1;  }
                }
                marked_row = Some( s_pointer );
            }

            if cmp_pairs( &r_join_elements[r_pointer], &s_join_elements[s_pointer] ) == Ordering::Equal {
                let r1 =  self.get_row(r_join_elements[r_pointer].row_index).unwrap();
                let r2 = other.get_row(s_join_elements[s_pointer].row_index).unwrap();
                join_table.insert_row( &join_rows(r1, r2, &column_to_join) )?;
                r_ptr_in_result = true;
                s_pointer += 1;
            } else {
                s_pointer  = marked_row.unwrap();
                r_ptr_in_result = false;
                r_pointer += 1;
                marked_row = None;
            }
        } 

        if r_pointer == r_join_elements.len() {
            return Ok(join_table)
        }

        while r_pointer != r_join_elements.len() {
            // make sure the last element of r wasn't used in the result
            if r_ptr_in_result { r_pointer += 1; r_ptr_in_result = false; continue; } 

            skipped_rows.push( (&r_join_elements[r_pointer]).row_index );
            r_pointer += 1;
        }

        // add any skipped rows in R to the join result with `NULL` values in the columns from S
        for row_index in skipped_rows {
            let mut r = self.rows().get( row_index ).unwrap().clone();
            for column in other.columns() {
                if column.get_name() == &column_to_join { continue; } // exists in R!
                r.insert( column.get_name().to_string(), FieldValue::Null );
            } 
            join_table.insert_row(&r)?;
        }        

        return Ok(join_table)
    }



    /// based on the algorithm from UCBerkley CS186: https://www.youtube.com/watch?v=jiWCPJtDE2c
    pub fn inner_join(&self, other: &Table, column_to_join: String) -> Result<Table, DBError> {
        
        #[derive(Debug)]
        struct JoinPair { value_to_sort_on: FieldValue, row_index: usize }

        fn cmp_pairs(p1: &JoinPair, p2: &JoinPair) -> Ordering {
            p1.value_to_sort_on.cmp(&p2.value_to_sort_on)
        }
        fn join_rows(r1: &HashMap<String, FieldValue>, r2: &HashMap<String, FieldValue>, join_column: &String) -> HashMap<String, FieldValue> {
            let mut result = r1.clone();
            for (k, v) in r2 {
                if k == join_column { continue; }
                result.insert(k.to_string(), v.clone());
            }
    
            result
        }

        let mut join_table_columns: Vec<Column> = Vec::new();

        for col in self.columns() {
            if col.get_name() == column_to_join { continue; }
            join_table_columns.push( col.clone() );
        }
        for col in other.columns() {
            if col.get_name() == column_to_join {
                let mut c = col.clone();
                c.change_pk_state( false );
                join_table_columns.push( c );
            } else {
                join_table_columns.push( col.clone() );
            }
        }

        let mut join_table: Table = Table::new(
            format!("Join Result of Tables {} and {} on column {}", self.name(), other.name(), &column_to_join),
            join_table_columns,
            false
        );


        // make sure there's at least one element
        if self.rows().len() == 0 || other.rows().len() == 0 {
            return Ok(join_table)
        }

        let mut r_join_elements: Vec<JoinPair> = Vec::new();
        let mut s_join_elements: Vec<JoinPair> = Vec::new();

        for (idx, r) in self.rows().iter().enumerate() {
            let field_value = r.get(&column_to_join).unwrap();
            r_join_elements.push( JoinPair{ value_to_sort_on: field_value.clone(), row_index: idx} );
        } 
        for (idx, r) in other.rows().iter().enumerate() {
            let field_value = r.get(&column_to_join).unwrap();
            s_join_elements.push( JoinPair{ value_to_sort_on: field_value.clone(), row_index: idx} );
        }

        
        r_join_elements.sort_by(|a, b| cmp_pairs(a, b) );
        s_join_elements.sort_by(|a, b| cmp_pairs(a, b) );
        

        let mut marked_row: Option<usize> = None;
        let mut r_pointer: usize = 0;
        let mut s_pointer: usize = 0;


        'outer: loop {
            // stop when one list ran out of elements
            if r_pointer == r_join_elements.len() || s_pointer == s_join_elements.len() {
                break 'outer;
            }

            if marked_row.is_none() {

                'until_eq: loop {
                    let row_cmp_result = cmp_pairs(&r_join_elements[r_pointer], &s_join_elements[s_pointer]);
                    if row_cmp_result == Ordering::Equal     { break 'until_eq; }
                    else if row_cmp_result == Ordering::Less { r_pointer += 1;  }
                    else /* if r > s */                      { s_pointer += 1;  }
                }
                marked_row = Some( s_pointer );
            }

            if cmp_pairs( &r_join_elements[r_pointer], &s_join_elements[s_pointer] ) == Ordering::Equal {
                let r1 =  self.get_row(r_join_elements[r_pointer].row_index).unwrap();
                let r2 = other.get_row(s_join_elements[s_pointer].row_index).unwrap();
                join_table.insert_row( &join_rows(r1, r2, &column_to_join) )?;
                s_pointer += 1;
            } else {
                s_pointer  = marked_row.unwrap();
                r_pointer += 1;
                marked_row = None;
            }
        } 

        return Ok(join_table)
    }

}

