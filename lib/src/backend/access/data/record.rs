use std::{rc::Rc, sync::atomic::{AtomicU8, Ordering}};

use super::value::{FieldValue, MAX_SIZE_OF_STRING};


static RECORD_ID_COUNTER: AtomicU8 = AtomicU8::new(0);
fn next_record_id() -> u8 { RECORD_ID_COUNTER.fetch_add(1, Ordering::Relaxed) }




#[derive(Debug, Clone)]
pub struct Record { 
    id: u8,
    data: Vec<FieldValue>
}



impl Record {
    pub fn id(&self) -> u8 { self.id }
    pub fn reassign_id(&mut self, newid: u8) {self.id = newid}
    pub fn data_as_mut(&self) -> Vec<FieldValue>     { self.data.clone()    }
    pub fn data_immut(&self)  -> Rc<Vec<FieldValue>> { Rc::new( self.data.clone() ) }
    
    pub fn empty() -> Self { Record{ id: 0, data: Vec::new()}}

    pub fn new(data: Vec<FieldValue> ) -> Self {

        let record_id = next_record_id();
        for datum in &data {
            match datum {
                FieldValue::STRING(v) => { 
                    if v.len() > MAX_SIZE_OF_STRING { panic!("No support for Strings longer than 20 chars yet.") } 
                },
                _ => continue
            }
        }

        Record { id: record_id, data }
    }
    
    pub fn to_binary(&self) -> Vec<u8> { 
        let mut serialized_data: Vec<u8> = vec![ self.id ]; // don't forget to add the records id!
        for datum in &self.data {
            let type_flag = datum.as_binary();
            let clean_up_strings = true;
            let mut serialized = datum.serialize(clean_up_strings).unwrap();
            serialized_data.push( type_flag );
            serialized_data.append( &mut serialized ); 
        }
        // println!("data: {:?}", &self.data);
        serialized_data
    }

    pub fn from_binary(data: Vec<u8>) -> Option<Record> {
        
        let mut values: Vec<FieldValue> = Vec::new();
        let record_id = data[0];
        let mut skip_to = 1;

        for (idx, datum) in data.iter().enumerate() {
            if idx < skip_to { continue; }
            match datum { // type flag
                0b00000000 => { // --- NULL ---
                    skip_to = idx + 1;
                    values.push( FieldValue::NULL );
                },
                0b00000001 => { // --- NUMBER ---
                    skip_to = idx + 5;
                    values.push( FieldValue::deserialize( data[idx..skip_to].to_vec() ).unwrap() );
                },
                0b00000010 => { // --- FLOAT ---
                    skip_to = idx + 5;
                    values.push( FieldValue::deserialize( data[idx..skip_to].to_vec() ).unwrap() );
                },
                0b00000100 => { // --- STRING ---
                    skip_to = idx + MAX_SIZE_OF_STRING + 1;
                    let string_data = data[idx..skip_to].to_vec();
                    values.push( FieldValue::deserialize( string_data ).unwrap() );
                },
                0b00001000 => { // --- BOOL ---
                    skip_to = idx + 2;
                    values.push( FieldValue::BOOL( data[skip_to-1] != 0 ) );
                },
                _ => return None
            }
        } 
        Some( Record { id: record_id, data: values } )
    }
}