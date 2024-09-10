use core::fmt;
use std::cmp::Ordering;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use super::db_err::DBError;
use url::Url;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
    is_primary_key: bool
}


impl Column {
    pub fn new(name: String, data_type: DataType, is_primary_key: bool) -> Self {
        Column {name:name, data_type:data_type, is_primary_key:is_primary_key }
    }

    pub fn get_name(&self)       -> &str   { &self.name }
    pub fn get_data_type(&self)  -> &DataType { &self.data_type }
    pub fn is_primary_key(&self) -> bool     { self.is_primary_key }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataType {
    String,
    Number,
    Date,
    Url,
    Boolean
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FieldValue {
    String(String),
    Number(f64),
    Date(DateTime<Utc>),
    Url(String),
    Boolean(bool),
    Null
}


/// this is gonna be such a piece of shit to unwrap
/// 
/// # NOTE
/// If a datatype is not found, NUMBER will be returned
/// 
/// returns the corresponding datatype for the following:
/// 1. "number" -> Number
/// 2. "date" -> Date
/// 3. "url" -> Url
/// 4. "boolean" OR "bool" -> Boolean
/// 5. "string" OR "str" -> String
pub fn parse_str(str: &str) -> DataType {
    match str.trim().to_lowercase().as_str() {
        "number"           => return DataType::Number,
        "date"             => return DataType::Date,
        "url"              => return DataType::Url, 
        "boolean" | "bool" => return DataType::Boolean,
        "string" | "str"   => return DataType::String,
        _ => DataType::Number
    } 
}


/// given a String, will return which datatype it can best fit into
/// will try all datatypes before returning `String`
///
/// ## Usage
/// used for parsing a users input into a datatype
pub fn parse_into_datatype(value: &String) -> FieldValue {
    // 1. Try parsing as Boolean
    let lower_value = value.to_lowercase();
    if lower_value == "true" {
        return FieldValue::Boolean(true);
    } else if lower_value == "false" {
        return FieldValue::Boolean(false);
    }

    // 2. Try parsing as Number (f64)
    if let Ok(num) = value.parse::<f64>() {
        return FieldValue::Number(num);
    }

    // 3. Try parsing as Date (YYYY-MM-DD)
    if let Ok(naive_date) = NaiveDate::parse_from_str(&value, "%Y-%m-%d") {
        // Combine date with default time "00:00:00"
        let date_str = format!("{}-{}-{} 00:00:00", naive_date.year(), naive_date.month(), naive_date.day());
        let date = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S").unwrap();
        return FieldValue::Date(Utc.from_utc_datetime(&date));
    }

    // 4. Try parsing as URL
    if let Ok(parsed_url) = Url::parse(&value) {
        return FieldValue::Url(Into::<String>::into(parsed_url));
    }

    // 5. If none of the above, return as String
    FieldValue::String(value.to_string())
}


impl FieldValue {
    pub fn is_number(&self) -> bool {
        match self {
            Self::Number(_) => true,
            _ => false
        }
    } 

    pub fn is_date(&self) -> bool {
        match self {
            Self::Date(_) => true,
            _ => false
        }
    }

    /// determines if the `self` enum value is less than `other`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn is_less_than(&self, other: &FieldValue) -> Result<bool, DBError> {
        match (self, other) {
            ( FieldValue::Number(v1), FieldValue::Number(v2) ) => Ok(v1 < v2),
            _ => {
                if self.data_type().eq(&DataType::Number) {
                    return Err(DBError::MisMatchDataType(DataType::Number, other.data_type()));
                }
                else {
                    return Err(DBError::MisMatchDataType(DataType::Number, self.data_type()));
                }
            }
        }
    }

    /// determines if the `self` enum value is less than or equal to `other`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn is_leq(&self, other: &FieldValue) -> Result<bool, DBError> {
        match (self, other) {
            ( FieldValue::Number(v1), FieldValue::Number(v2) ) => Ok(v1 <= v2),
            _ => {
                if self.data_type().eq(&DataType::Number) {
                    return Err(DBError::MisMatchDataType(DataType::Number, other.data_type()));
                }
                else {
                    return Err(DBError::MisMatchDataType(DataType::Number, self.data_type()));
                }
            }
        }
    }

    /// determines if the `self` enum value is greater than `other`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn is_greater_than(&self, other: &FieldValue) -> Result<bool, DBError> {
        match (self, other) {
            ( FieldValue::Number(v1), FieldValue::Number(v2) ) => Ok(v1 > v2),
            _ => {
                if self.data_type().eq(&DataType::Number) {
                    return Err(DBError::MisMatchDataType(DataType::Number, other.data_type()));
                }
                else {
                    return Err(DBError::MisMatchDataType(DataType::Number, self.data_type()));
                }
            }
        }
    }

    /// determines if the `self` enum value is greater than or equal to `other`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn is_geq(&self, other: &FieldValue) -> Result<bool, DBError> {
        match (self, other) {
            ( FieldValue::Number(v1), FieldValue::Number(v2) ) => Ok(v1 >= v2),
            _ => {
                if self.data_type().eq(&DataType::Number) {
                    return Err(DBError::MisMatchDataType(DataType::Number, other.data_type()));
                }
                else {
                    return Err(DBError::MisMatchDataType(DataType::Number, self.data_type()));
                }
            }
        }
    }

    /// determines if the `self` enum value is between the values `other1` and `other2`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn is_between(&self, o1: f64, o2: f64) -> Result<bool, DBError> {
        match (o1, self, o2) {
            ( v0, FieldValue::Number(v1), v2) => Ok((v0 <= *v1) && (*v1 <= v2)),
            _ => return Err(DBError::MisMatchDataType(DataType::Number, self.data_type()))
        }
    }

    /// determines if the `self` enum value is between the dates `other1` and `other2`.
    /// 
    /// 
    /// ## IMPORTANT NOTE: 
    /// **DOES NOT RETURN AN ERROR IF THE WRONG DATATYPE IS PUT IN**
    /// 
    /// make the check for yourself!  
    pub fn date_is_between(&self, o1: DateTime<Utc>, o2: DateTime<Utc>) -> Result<bool, DBError> {
        match (o1, self, o2) {
            ( d0, FieldValue::Date(d1), d2) => Ok((&d0 < d1) && (d1 < &d2)),
            _ => return Err(DBError::MisMatchDataType(DataType::Date, self.data_type()))
        }
    }


    /// # NOTE
    /// returns a **NUMBER DATATYPE** if a field value is null
    pub fn data_type(&self) -> DataType {
        match self {
            FieldValue::String(_) =>  DataType::String,
            FieldValue::Number(_) =>  DataType::Number,
            FieldValue::Date(_) =>    DataType::Date,
            FieldValue::Url(_) =>     DataType::Url,
            FieldValue::Boolean(_) => DataType::Boolean,
            FieldValue::Null =>       DataType::Number,
        }
    }


    pub fn compare_to(&self, other: &FieldValue ) -> Result<Ordering, DBError> {

        match (self, other) {
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => {
                if (!a && !b) || (*a && *b) {    
                    return Ok(Ordering::Equal);
                } else if *a && !b { // a is True (1) and b is False (0) 
                    return Ok(Ordering::Greater)
                } else {    // a is False (0) and b is True (1)
                    return Ok(Ordering::Less)
                }
            }
            (FieldValue::Date(a), FieldValue::Date(b)) => {
                if a < b { 
                    return Ok(Ordering::Less) 
                } else if a == b {
                    return Ok(Ordering::Equal)
                } else {
                    return Ok(Ordering::Greater)
                }
            },
            (FieldValue::Number(a), FieldValue::Number(b)) => {
                if a < b { 
                    return Ok(Ordering::Less) 
                } else if a == b {
                    return Ok(Ordering::Equal)
                } else {
                    return Ok(Ordering::Greater)
                }
            },
            (FieldValue::String(a), FieldValue::String(b)) => {
                if a < b { 
                    return Ok(Ordering::Less)
                } else if a == b {
                    return Ok(Ordering::Equal)
                } else {
                    return Ok(Ordering::Greater)
                }
            },
            (FieldValue::Url(a), FieldValue::Url(b)) => {
                return Ok(a.cmp(&b))
            },
            (FieldValue::Null, FieldValue::Null) => { return Ok(Ordering::Equal) },
            _ => return Err(DBError::MisMatchDataType(self.data_type(), other.data_type()))
        }

    }
}



impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::String(_)  => write!(f, "String"),
            FieldValue::Number(_)  => write!(f, "Number"),
            FieldValue::Date(_)    => write!(f, "Date"),
            FieldValue::Url(_)     => write!(f, "Url"),
            FieldValue::Boolean(_) => write!(f, "Boolean"),
            FieldValue::Null       => write!(f, "Null")
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::String  => write!(f, "String"),
            DataType::Number  => write!(f, "Number"),
            DataType::Date    => write!(f, "Date"),
            DataType::Url     => write!(f, "Url"),
            DataType::Boolean => write!(f, "Boolean"),
        }
    }
}


impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::String, DataType::String)   => true,
            (DataType::Number, DataType::Number)   => true,
            (DataType::Date, DataType::Date)       => true,
            (DataType::Url, DataType::Url)         => true,
            (DataType::Boolean, DataType::Boolean) => true,
            _ => false
        }
    }
}


impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Number(l0), Self::Number(r0)) => l0 == r0,
            (Self::Date(l0), Self::Date(r0)) => l0 == r0,
            (Self::Url(l0), Self::Url(r0)) => l0 == r0,
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            _ => false,
        }
    }
}