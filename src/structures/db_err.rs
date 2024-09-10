use core::fmt;
use std::error::Error;

use super::{column::DataType, modify_where::FilterCondition};



// TODO: change this to something more generic, maybe DatabaseError or something like that?
#[derive(Debug)]
pub enum DBError {

    PrimaryKeyRequired,

    MissingPrimaryKeys(Vec<String>),

    /// first is expected dt, second is actual
    MisMatchDataType(DataType, DataType),

    InvalidColumn(String),

    MissingModifyCriteria(FilterCondition),

    // primary key column name
    DuplicatePrimaryKey(String),

    // thrown if a user tries to delete a primary key column
    MandatoryColumn(String)
}


impl Error for DBError {}


impl fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::PrimaryKeyRequired
                => write!(f, "at least one primary key is required for this operation"),
            DBError::MissingPrimaryKeys(pks) 
                => write!(f, "the following primary keys were missing: {}", pks.join(", ")),
            DBError::MisMatchDataType(expected, actual) 
                => write!(f, "expected datatype '{}', but got '{}'", expected, actual),
            DBError::InvalidColumn(name) 
                => write!(f, "the column '{}' does not exist in the database", name),
            DBError::MissingModifyCriteria(modify_type) 
                => write!(f, "the row modify method '{}' is missing a value", modify_type),
            DBError::DuplicatePrimaryKey(pk_col_name) 
                => write!(f, "primary key value already exists in the column '{}'", pk_col_name),
            DBError::MandatoryColumn(col_name) 
                => write!(f, "The column '{}' is a requirement for this or other tables.", col_name)
        }
    }
}