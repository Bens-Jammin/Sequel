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


#[derive(Serialize, Deserialize, Clone, Debug)]
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


    pub fn serialize(&self) -> Option<Vec<u8>> {
        match &self {
            Self::NULL               => return Some(vec![]),
            Self::FLOAT(v)     => return Some((*v).to_ne_bytes().to_vec()),
            Self::STRING(s) => 
                if s.len() <= MAX_SIZE_OF_STRING { 
                    let mut d = s.clone().into_bytes();
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
}