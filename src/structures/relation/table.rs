use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::structures::column::{Column, FieldValue};


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub(super) name: String,
    pub(super) columns: Vec<Column>,
    pub(super) primary_keys: Vec<Column>,
    pub(super) rows: Vec<HashMap<String, FieldValue>>,
}