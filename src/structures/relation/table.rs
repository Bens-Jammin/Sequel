use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::structures::column::{Column, FieldValue};

// TODO: implement pages
/**
- static sized ints, strings, etc
- implement my own date struct
*/


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub(super) name: String,
    pub(super) columns: Vec<Column>,
    pub(super) primary_keys: Vec<Column>,
    pub(super) rows: Vec<HashMap<String, FieldValue>>,
}