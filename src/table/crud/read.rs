use std::collections::HashMap;

use crate::access::data::value::FieldValue;


/// how do most databases store table metadata? how do they know that 
/// data types match up and which ones should be NULLed out?
pub fn insert_row(table_name: &str, data: HashMap<String, FieldValue>) {
    
}