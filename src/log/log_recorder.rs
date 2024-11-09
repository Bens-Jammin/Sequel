use std::{fs::OpenOptions, io::Write};

use chrono::{DateTime, Utc};

use crate::structures::{column::FieldValue, db_err::DBError};


// TODO: test this
pub struct LogRecorder {
    buffer_size: u8,
    file_path: String,
    buf: Vec<LogEntry>
}

#[derive(Clone)]
pub struct LogEntry{
    timestamp: DateTime<Utc>
}


enum LogType {
    /// Significant or noteworthy events
    INFO,
    /// Warn about situations which may warn of future problems
    WARN,
    /// Unrecoverable patterns that affect one operation
    ERROR,
    /// Unrecoverable arrors that affect the whole program
    FATAL,
    
    DEBUG

}


impl LogEntry {
    pub fn new() -> Self {
        let timestamp = chrono::offset::Utc::now();
        LogEntry { timestamp }
    }


    fn to_string(&self) -> String { 
        format!(
            "< {} | Collecting data for log not supported right now >", 
            self.timestamp
        )
    }
}

impl LogRecorder {
    pub fn new(buffer_size: u8, file_path: String ) -> Self 
    { LogRecorder { buffer_size, file_path, buf: Vec::with_capacity( buffer_size as usize ) } }


    pub fn log_entry( &mut self, entry: LogEntry ) -> Result<(), DBError> {
        if self.buf.len() >= self.buffer_size.into() {
            self.flush()?;
        } 
        // insert log element to buffer
        self.buf.push(entry);
        Ok(())
    }


    fn flush(&mut self) -> Result<(), DBError> {

        let mut log_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)
            .unwrap();
        
        let content: String = self.buf
            .iter()
            .map(|e| 
                format!("{}",e.to_string()
            ))
            .collect::<Vec<String>>()
            .join("\n");  

        // this is the only actual 'function' of this function,
        // the others above are just formatting
        log_file.write_all(content.as_bytes()).unwrap();
        self.buf.clear();

        Ok(())
    }
}