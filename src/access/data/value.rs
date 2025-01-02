#[derive(Debug, Clone)]
pub enum FieldValue {
    NUMBER(u32),
    FLOAT(f32),
    STRING(String),
    BOOL(bool),
    NULL
}


#[derive(Debug)]
pub enum ColumnType {
    NUMBER,
    FLOAT,
    STRING,
    BOOLEAN
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
                if s.len() <= 20 { 
                    let mut d = s.clone().into_bytes();
                    d.append( 
                        &mut (0..(20-d.len())) // find the size of 0's needed to get to 20 chars
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
                if data[1..].len() > 20 { return  None; } // Strings must be 20 chars or less
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