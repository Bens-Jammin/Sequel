use std::fmt::Display;



pub enum DatabaseError {
    /// 1. file or dir 
    /// 2. operation that failed
    IoError(String, String)
} 


impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

// NOTE: make a table struct that acts like cursive structs, don't use standalone functions
