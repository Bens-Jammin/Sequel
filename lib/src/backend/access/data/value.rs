use std::{cmp::Ordering, fmt::Display};

use serde::{Deserialize, Serialize};


#[derive(Debug, Clone)]
pub enum FieldValue {
    NUMBER(u32),
    FLOAT(f32),
    STRING(String),
    BOOL(bool),
    NULL
}


#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ColumnType {
    NUMBER,
    FLOAT,
    STRING,
    BOOLEAN
}


pub static MAX_SIZE_OF_STRING: usize = 64;



impl ColumnType {
    pub fn from_str(s: &str) -> Option<ColumnType> {
        match s.to_lowercase().as_str() {
            "number" | "num"   => Some(ColumnType::NUMBER),
            "float"            => Some(ColumnType::FLOAT),
            "string" | "str"   => Some(ColumnType::STRING),
            "boolean" | "bool" => Some(ColumnType::BOOLEAN),
            _                  => None
        }
    } 

    
    /// This function is used to allow FieldValue to act as an inferred type when parsing data such as CSVs.
    /// If the current value is less than the value to compare against (the value found in a column),
    /// then the inferred type will upgrade to the most 'open' value allowed (upgrading to the compared value)
    /// 
    /// The order is: <br>
    /// 1. Boolean <br>
    /// 2. Integer <br>
    /// 3. Float <br>
    /// 4. String
    pub(crate) fn upgrade(inferred_type: Option<&ColumnType>, compared_value: &FieldValue) -> ColumnType {
        
        match inferred_type {
            Some(v) => match v{
                ColumnType::BOOLEAN => match compared_value { FieldValue::BOOL(_) => ColumnType::BOOLEAN, _ => ColumnType::STRING },
                ColumnType::NUMBER => match compared_value { 
                    FieldValue::NUMBER(_) => ColumnType::NUMBER,
                    FieldValue::FLOAT(_) => ColumnType::FLOAT,
                    _ => ColumnType::STRING
                },
                ColumnType::FLOAT => match compared_value { FieldValue::FLOAT(_) => ColumnType::FLOAT, _ => ColumnType::STRING },
                ColumnType::STRING => ColumnType::STRING,
            }
            None => compared_value.column_type()
        }
    }
}



impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::NUMBER  => write!(f, "NUMBER"),
            ColumnType::FLOAT   => write!(f, "FLOAT"),
            ColumnType::STRING  => write!(f, "STRING"),
            ColumnType::BOOLEAN => write!(f, "BOOLEAN"),
        }
    }
}


impl Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::NUMBER(v) => write!(f, "{v}"),
            FieldValue::FLOAT(v) => write!(f, "{v}"),
            FieldValue::STRING(v) => write!(f, "{v}"),
            FieldValue::BOOL(v) => write!(f, "{v}"),
            FieldValue::NULL => write!(f, "NULL"),
        }
    }
}


impl PartialOrd for FieldValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (FieldValue::NUMBER(v1), FieldValue::NUMBER(v2)) => 
                { Some(v1.cmp(v2)) },
            (FieldValue::NUMBER(v1), FieldValue::FLOAT(v2)) => 
                { 
                Some(if (*v1 as f32) > *v2 { Ordering::Greater } 
                else if (*v1 as f32) > *v2 { Ordering::Equal} 
                   else                    { Ordering::Less } ) 
                },
            (FieldValue::FLOAT(v1), FieldValue::NUMBER(v2)) => 
                { 
                Some(if (*v2 as f32) > *v1 { Ordering::Greater } 
                else if (*v2 as f32) > *v1 { Ordering::Equal} 
                   else                    { Ordering::Less } ) 
                },
            (FieldValue::FLOAT(v1), FieldValue::FLOAT(v2)) => 
                { 
                Some(if (*v1 as f32) > *v2 { Ordering::Greater } 
                else if (*v1 as f32) > *v2 { Ordering::Equal} 
                   else                    { Ordering::Less } ) 
                },
            (FieldValue::STRING(v1), FieldValue::STRING(v2)) => 
                { Some(v1.cmp(v2)) },
            (FieldValue::BOOL(v1), FieldValue::BOOL(v2)) => 
                { Some(v1.cmp(v2)) },
            _ => None
        }
    }
}


impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NUMBER(l0), Self::NUMBER(r0)) => l0 == r0,
            (Self::NUMBER(v1), Self::FLOAT(v2)) => (*v1 as f32) == *v2,
            (Self::NUMBER(v1), Self::BOOL(v2)) => *v1 == (*v2 as u32),
            (Self::FLOAT(l0), Self::FLOAT(r0)) => l0 == r0,
            (Self::FLOAT(v1), Self::BOOL(v2)) =>*v1 == (*v2 as u32 as f32),
            (Self::STRING(l0), Self::STRING(r0)) => l0.eq(r0),
            (Self::STRING(v1), Self::BOOL(v2)) => v1.len() == (*v2 as usize),
            (Self::BOOL(l0), Self::BOOL(r0)) => l0 == r0,
            (Self::BOOL(v1), Self::NUMBER(v2)) => *v2 == (*v1 as u32),
            (Self::BOOL(v1), Self::FLOAT(v2)) => *v2 == (*v1 as u32 as f32),
            (Self::BOOL(v1), Self::STRING(v2)) => v2.len() == (*v1 as usize),
            (Self::NULL, Self::NULL) => true,
            // ? tf is this
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
    
    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}



impl FieldValue {
    
    /// Returns the byte representation of the values type.
    /// 
    /// Each bit in the byte refers to a different data type:
    /// 
    /// |  type  | binary representation |
    /// | ------ | --------------------- |
    /// | `NULL`   | 00000000            |
    /// | `NUMBER` | 00000001            |
    /// | `FLOAT`  | 00000010            |
    /// | `STRING` | 00000100            |
    /// | `BOOL`   | 00001000            | 
    pub fn as_binary(&self) -> u8 {
        match &self {   // storing the FV representation as a bit map bc i dont want to deal with sizes < 8 bits
            Self::NULL      => return 0b00000000,  
            Self::NUMBER(_) => return 0b00000001,
            Self::FLOAT(_)  => return 0b00000010,
            Self::STRING(_) => return 0b00000100,
            Self::BOOL(_)   => return 0b00001000,
        }
    }


    pub fn parse(obj: &str) -> FieldValue {
        
        let cleaned_obj = obj.trim().to_ascii_uppercase(); 
        
        if cleaned_obj == "true" { return FieldValue::BOOL(true) }
        if cleaned_obj == "false" { return FieldValue::BOOL(false) }
        if cleaned_obj == "null" { return FieldValue::NULL }

        match cleaned_obj.parse::<u32>() {
            Ok(v) => return FieldValue::NUMBER(v),
            Err(_) => (),
        }

        match cleaned_obj.parse::<f32>() {
            Ok(v) => return FieldValue::FLOAT(v),
            Err(_) => (),
        }

        // if all else fails just return the object as a string value 
        FieldValue::STRING( obj.to_string() )
    }


    /// converts a field value (aka cell) into its binary representation.
    /// The `clean string` parameter removes and leading/trailing spaces and removes double quotes
    /// 
    /// ex: `" "This is a string"` becomes  `"This is a string"`
    pub fn serialize(&self, clean_string: bool) -> Option<Vec<u8>> {
        match &self {
            Self::NULL               => return Some(vec![]),
            Self::FLOAT(v)     => return Some((*v).to_ne_bytes().to_vec()),
            Self::STRING(s) => 
                if s.len() <= MAX_SIZE_OF_STRING { 

                    let cleaned_str = if clean_string { s.trim().trim_matches('"').to_string() } 
                                                         else { s.to_string() };

                    let mut d = cleaned_str.clone().into_bytes();
                    d.append( 
                        &mut (0..(MAX_SIZE_OF_STRING-d.len())) // find the size of 0's needed to get to 20 chars
                        .map(|_| 0) // fill them with zeros
                        .collect::<Vec<u8>>()  // collect into a vector and append to `d`
                    );
                    return Some( d ) 
                } 
                else { return None },
            Self::BOOL(v)     => return Some(vec![*v as u8]),
            Self::NUMBER(v)    => return Some(vec![
                (v >> 24) as u8,
                (v >> 16) as u8,
                (v >> 08) as u8,
                *v        as u8
            ])
        }  
    }


    pub fn deserialize(data: Vec<u8>) -> Option<FieldValue> {
        match data[0] { // type flag
            0b00000000 => return Some(FieldValue::NULL),
            0b00000001 => return Some(FieldValue::NUMBER( 
                ((data[1] as u32) << 24) 
              + ((data[2] as u32) << 16)
              + ((data[3] as u32) << 08)
              +  (data[4] as u32       ))), 
            0b00000010 => return Some(FieldValue::FLOAT( 
                f32::from_ne_bytes([data[1], data[2], data[3], data[4]]) 
            )),
            0b00000100 => {
                if data[1..].len() > MAX_SIZE_OF_STRING { return  None; } // Strings must be 20 chars or less
                let mut result_string = data[1..]
                    .iter()
                    .map(|byte| 
                        *byte as char
                    ).collect::<String>();
    
                // remove the padded 0's from the serialization
                result_string = result_string.replace('\0', "");
    
                return Some(FieldValue::STRING( result_string )) 
            },
            0b00001000 => return Some(FieldValue::BOOL(data[1] != 0)),
            _ => return None
        }
    }


    pub(crate) fn column_type(&self) -> ColumnType {
        match self {
            FieldValue::NUMBER(_) => ColumnType::NUMBER,
            FieldValue::FLOAT(_)  => ColumnType::FLOAT,
            FieldValue::BOOL(_)   => ColumnType::BOOLEAN,
            FieldValue::STRING(_) => ColumnType::STRING,
            FieldValue::NULL      => ColumnType::STRING,
        }
    }


    /// creates a new field value with the type aligning with the given column type
    pub fn convert_to(&self, column_type: &ColumnType) -> FieldValue {
        match column_type {
            ColumnType::BOOLEAN => match self {
                FieldValue::BOOL(b) => FieldValue::BOOL(*b),
                FieldValue::NUMBER(n) => FieldValue::BOOL(*n != 0),
                FieldValue::FLOAT(f) => FieldValue::BOOL(*f != 0.0),
                FieldValue::STRING(s) => FieldValue::BOOL(s != "false" && s != "0" && !s.is_empty()),
                FieldValue::NULL => panic!("Cannot convert a value of type NULL into a boolean"),
            },
            ColumnType::NUMBER => match self {
                FieldValue::BOOL(b) => FieldValue::NUMBER(if *b { 1 } else { 0 }),
                FieldValue::NUMBER(n) => FieldValue::NUMBER(*n),
                FieldValue::FLOAT(f) => FieldValue::NUMBER(*f as u32),
                FieldValue::STRING(s) => FieldValue::NUMBER(s.parse::<u32>().unwrap()),
                FieldValue::NULL => panic!("Cannot convert a value of type NULL into a number"),
            },
            ColumnType::FLOAT => match self {
                FieldValue::BOOL(b) => FieldValue::FLOAT(if *b { 1.0 } else { 0.0 }),
                FieldValue::NUMBER(n) => FieldValue::FLOAT(*n as f32),
                FieldValue::FLOAT(f) => FieldValue::FLOAT(*f),
                FieldValue::STRING(s) => FieldValue::FLOAT(s.parse::<f32>().unwrap()),
                FieldValue::NULL => panic!("Cannot convert a value of type NULL into a float"),
            },
            ColumnType::STRING => match self {
                FieldValue::BOOL(b) => FieldValue::STRING(b.to_string()),
                FieldValue::NUMBER(n) => FieldValue::STRING(n.to_string()),
                FieldValue::FLOAT(f) => FieldValue::STRING(f.to_string()),
                FieldValue::STRING(s) => FieldValue::STRING(s.clone()),
                FieldValue::NULL => panic!("Cannot convert a value of type NULL into a string"),
            },
        }
    }

}