use std::{cmp::Ordering, collections::HashMap};

use crate::structures::{column::FieldValue, db_err::DBError, sort::SortCondition};

use super::table::*;


impl Table {

    pub fn sort_rows(&mut self, sorting_by: SortCondition, sorting_column: String) -> Result<(), DBError> {
        
        if !self.is_valid_column( &sorting_column ) {
            return Err(DBError::InvalidColumn( sorting_column.clone() ));
        }

        fn compare(col: &String, a: &HashMap<String, FieldValue>, b: &HashMap<String, FieldValue> , descending_ord: bool) -> Ordering {
            let a = a.get(col).unwrap();
            let b = b.get(col).unwrap();
            let comparison_result = if descending_ord { b.compare_to(a) } else {a.compare_to(b) };
            match comparison_result {
                Ok(ordering) => ordering,
                // temporary, unsure what to do if an error is thrown right now, if its even possible with this implementation 
                Err(_) => Ordering::Equal   
            }
        }

        match sorting_by {
            SortCondition::NumericAscending  => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, false)),
            SortCondition::NumericDescending => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, true)),
            SortCondition::AlphaAscending    => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, false)),
            SortCondition::AlphaDescending   => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, true)),
            SortCondition::DateAscending     => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, false)),
            SortCondition::DateDescending    => self.rows.sort_by(|a, b| compare(&sorting_column, a, b, true)),
        };


    
        Ok(())
    }


}