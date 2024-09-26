use core::fmt;
use std::collections::HashMap;
use crate::structures::{
    self, column::{parse_into_field_value, parse_str, Column, DataType, FieldValue}, db_err::DBError, modify_where::FilterCondition, sort_method::SortCondition, table::{self, load_database, Table}
};


#[derive(Debug)]
pub enum Query {
    /// SELECT (col1, col2, ..., coln) FROM (table)
    SELECT(Vec<String>, String),

    /// INSERT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
    INSERT(Vec<String>, String, Vec<String>),

    /// EDIT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
    EDIT(Vec<String>, String, Vec<String>),

    /// DELETE FROM (table) WHERE (column) (condition) (condition_value)
    DELETE(String, String, FilterCondition, FieldValue),

    /// SORT (table) ON (sort_condition) COLUMN (column)
    SORT(String, SortCondition, String),

    /// FILTER FROM (table) WHERE (column) (condition) (condition_value)
    FILTER(String, String, FilterCondition, FieldValue),

    /// INDEX (table) (column)
    INDEX(String, String),

    // CREATE (table_name) COLUMNS (col_name1:data_type1, etc) KEYS (col_name_1, etc)
    CREATE(String, Vec<String>, Vec<DataType>, Vec<String>),

}


pub fn list_queries() -> String {

    let mut query_list = String::from("\n");
    for (idx, q) in all_queries().iter().enumerate() {
        query_list += &format!("{}) {q}\n", idx+1);
    }
    query_list
}


fn all_queries() -> Vec<Query> {
    let s = String::new();
    let cs = vec![String::new()];
    let dts = vec![DataType::Number];
    let sc = SortCondition::AlphaAscending;
    let fc = FilterCondition::Null;
    let fv = FieldValue::Null;
    let fc2 = FilterCondition::Null;
    let fv2 = FieldValue::Null;


    vec![
        Query::SELECT(cs.clone(), s.clone()),
        Query::INSERT(cs.clone(), s.clone(), cs.clone()),
        Query::EDIT(cs.clone(), s.clone(), cs.clone()),
        Query::DELETE(s.clone(), s.clone(), fc, fv),
        Query::SORT(s.clone(), sc, s.clone()),
        Query::FILTER(s.clone(), s.clone(), fc2, fv2),
        Query::INDEX(s.clone(), s.clone()),
        Query::CREATE(s, cs.clone(), dts, cs)
    ]
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::SELECT(_, _) 
            => write!(f, "SELECT (col1, col2, ...) FROM {{table_name}}"),
            Query::INSERT(_, _, _) 
            => write!(f, "INSERT (val1, val2, ...) INTO {{table}} (col1, col2, ..."),
            Query::EDIT(_, _, _) 
            => write!(f, "EDIT (val1, val2, ..., valn) INTO {{table}} (col1, col2, ...)"),
            Query::DELETE(_, _, _, _) 
            => write!(f, "DELETE FROM {{table}} WHERE {{column}} {{condition}} {{condition_value}}"),
            Query::SORT(_, _, _)
             => write!(f, "SORT {{table}} ON {{sort_condition}} COLUMN {{column}}"),
            Query::FILTER(_, _, _, _)
             => write!(f, "FILTER FROM {{table}} WHERE {{column}} {{condition}} {{condition_value}}"),
            Query::INDEX(_, _)
             => write!(f, "INDEX {{table}} {{column}}"),
            Query::CREATE(_, _, _, _)
             => write!(f, "CREATE {{table_name}} COLUMNS (col_name1:data_type1, ...) KEYS (col_name_1, ...)"),
        }
    }
}


/// given a users command, converts it into a valid database query if possible.
/// returns None if there is an error during parsing.
/// 
/// ## Valid Query Templates
/// 
/// SELECT `(col1, col2, ..., coln)` FROM `(table)` <br>
/// INSERT `(val1, val2, ..., valn)` INTO `(table)` `(col1, col2, ..., coln)` <br>
/// EDIT `(val1, val2, ..., valn)` INTO `(table)` `(col1, col2, ..., coln)` <br>
/// REMOVE FROM `(table)` WHERE `(condition)` <br>
/// SORT `(table)` ON `(sort_condition)` COLUMN (column) <br>
/// FILTER `(table)` ON `(filter_condition)` <br>
/// INDEX `(table)` `(column)`
pub fn parse_query(command: String) -> Option<Query> {
    
    // Helper function to parse a comma-separated list within parentheses
    fn parse_list(input: &str) -> Vec<String> {
        input
            .trim_matches(|c| c == '(' || c == ')')
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }

    /// helper function to split the command into its parts, while keeping lists intact 
    fn split_outside_parentheses(s: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut start = 0;
        let mut inside_parentheses = false;

        let bytes = s.as_bytes(); // Work with bytes to track positions

        for (i, &c) in bytes.iter().enumerate() {
            match c {
                b'(' => inside_parentheses = true,
                b')' => inside_parentheses = false,
                b' ' => {
                    if !inside_parentheses {
                        if start != i { // Check if we have a non-empty word
                            result.push(&s[start..i]);
                        }
                        start = i + 1; // Update start to be after the space
                    }
                }
                _ => {} // Do nothing for other characters
            }
        }

        // Add the last word if there's any remaining after the last space
        if start < s.len() {
            result.push(&s[start..]);
        }

        result
    }

    // Trim the command and split it by whitespace
    let parts: Vec<&str> = split_outside_parentheses(&command);

    let main_query_command = parts[0].to_lowercase();

    // Match various command templates
    if main_query_command.starts_with("select") {
        // SELECT (col1, col2, ..., coln) FROM (table)
        if let Some(from_index) = parts.iter().position(|&s| s.to_lowercase() == "from") {
            let columns = parse_list(parts[1]);
            println!("parsed list: {:?} | parts: {:?}", &columns, &parts);
            let table = parts[from_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
            return Some(Query::SELECT(columns, table));
        }
    } else if main_query_command.starts_with("insert") {
        // INSERT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
        if let Some(into_index) = parts.iter().position(|&s| s.to_lowercase() == "into") {
            let values = parse_list(parts[1]);
            let table = parts[into_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
            let columns = parse_list(parts[into_index + 2]);
            return Some(Query::INSERT(values, table, columns));
        }
    } else if main_query_command.starts_with("edit") {
        // EDIT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
        if let Some(into_index) = parts.iter().position(|&s| s.to_lowercase() == "into") {
            let values = parse_list(parts[1]);
            let table = parts[into_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
            let columns = parse_list(parts[into_index + 2]);
            return Some(Query::EDIT(values, table, columns));
        }
    } else if main_query_command.starts_with("remove") {

        // REMOVE FROM (table) WHERE (column) (condition) (field_value)
        if let (Some(from_index), Some(where_index)) = ( 
            parts.iter().position(|&s| s.to_lowercase() == "from"), 
            parts.iter().position(|&s| s.to_lowercase() == "where")
        ) {
            let table = parts[from_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
            let column = parts[where_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();

            // Parse FilterCondition (e.g., LessThan, GreaterThan, etc.)
            let condition_str = parts[where_index + 2];
            let condition = FilterCondition::parse_str(condition_str);

            if let Some(cond) = condition {
                // Parse FieldValue (e.g., 42, "string", etc.)
                let field_value_str = String::from(parts[where_index + 3]);
                let field_value = parse_into_field_value(&field_value_str);

                // Return a valid DELETE query if all parts were successfully parsed
                return Some(Query::DELETE(table, column, cond, field_value));
            }
        }
    } else if main_query_command.starts_with("sort") {
        // SORT (table) ON (sort_condition)
        if let Some(on_index) = parts.iter().position(|&s| s.to_lowercase() == "on") {
            let table = parts[1].trim_matches(|c| c == '(' || c == ')').to_string();
            let sort_condition = SortCondition::parse_str( parts[on_index + 1] );
            
            if sort_condition.is_none() { return None }

            if let Some(column_index) = parts.iter().position(|&s| s.to_lowercase() == "column") {
                let column = parts[column_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
                
                return Some(Query::SORT(table, sort_condition.unwrap(), column));
            } else { return None }   
        }
    } else if main_query_command.starts_with("filter") {
        // FILTER FROM (table) WHERE (column) (condition) (condition_value)
        if let (Some(from_index), Some(where_index)) = ( 
            parts.iter().position(|&s| s.to_lowercase() == "from"), 
            parts.iter().position(|&s| s.to_lowercase() == "where")
        ) {
            let table = parts[from_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
            let column = parts[where_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();

            // Parse FilterCondition (e.g., LessThan, GreaterThan, etc.)
            let condition_str = parts[where_index + 2];
            let condition = FilterCondition::parse_str(condition_str);

            if let Some(cond) = condition {
                // Parse FieldValue (e.g., 42, "string", etc.)
                let field_value_str = String::from(parts[where_index + 3]);
                let field_value = parse_into_field_value(&field_value_str);

                return Some(Query::FILTER(table, column, cond, field_value));
            }
        }
    } else if main_query_command.starts_with("index") {
        // INDEX (table) (column)
        let table = parts[1].trim_matches(|c| c == '(' || c == ')').to_string();
        let column = parts[2].trim_matches(|c| c == '(' || c == ')').to_string();
        return Some(Query::INDEX(table, column));
    } else if main_query_command.starts_with("create") {
        // CREATE (table_name) COLUMNS (col_name1:data_type1, etc) KEYS (col_name_1, etc)
        if let Some(columns_index) = parts.iter().position(|&s| s.to_lowercase() == "columns") {
            let table_name = parts[1].trim_matches(|c| c == '(' || c == ')').to_string();
            
            let columns_str = parts[columns_index + 1];
            let columns_and_values: Vec<String> = columns_str.split(',').map(|s| s.trim().to_string()).collect();

            let mut column_names = Vec::new();
            let mut data_types = Vec::new();

            for pair in columns_and_values {
                let pair = pair.replace("(", "");
                let pair = pair.replace(")", "");
                let mut split = pair.split(':');
                let column_name = split.next().unwrap().to_string();
                let data_type_str = split.next().unwrap().to_string();
                column_names.push(column_name);

                // Parse Datatype
                data_types.push( parse_str(&data_type_str) );
            }

            if let Some(keys_index) = parts.iter().position(|&s| s.to_lowercase() == "keys") {
                let keys_str = parts[keys_index + 1];
                let keys: Vec<String> = parse_list(keys_str);

                return Some(Query::CREATE(table_name, column_names, data_types, keys));
            }
        }
    }

    // If no valid command is found, return None
    None
}


/// # NOTE 
/// local path must be where **ALL** files will be stored. Both relations **AND** indexes
pub fn execute_query(query: Query, save_dir: &str) -> Result<Either<Table, String>, DBError>{

    let relation_directory = format!("{}/relations", save_dir);
    let index_directory = format!("{}/indexes", save_dir);

    match query {
        Query::SELECT(col_names, table) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let db = structures::table::load_database(&file_path)?;

            let r = db.select_columns(&col_names)?;

            return Ok(Either::This(r))
        },
        Query::INSERT(new_vals, table, col_names) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let mut db = structures::table::load_database(&file_path)?;
            
            let mut row: HashMap<String, FieldValue> = HashMap::new();

            for (col_name, new_val) in col_names.iter().zip(new_vals) {
                let datatype = parse_into_field_value(&new_val);
                row.insert(col_name.to_owned(), datatype);
            }

            db.insert_row(row)?;
            db.save(relation_directory)?;

            return Ok(Either::This(db))
        },
        Query::EDIT(new_vals, table, col_names) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let db = structures::table::load_database(&file_path)?;
            // TODO: edit implementation
            // let r = db.edit_rows()

            // db.save();
        },
        Query::SORT(table, condition, column) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let mut db = structures::table::load_database(&file_path)?;
            
            db.sort_rows(condition, column)?;

            return Ok(Either::This(db))
        },
        Query::INDEX(table, column) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let db = structures::table::load_database(&file_path)?;
            let index = db.index_column(column.clone())?;
            
            table::save_index(&index_directory, &table, &column, index);
            // save index
            // return a message saying the index on {column} was created
            return Err(DBError::ActionNotImplemented("indexing a table".to_owned()))
        },
        Query::CREATE(table, col_names, datatypes, keys) => {
            let mut columns: Vec<Column> = Vec::new();
            for (col, datatype) in col_names.iter().zip(datatypes.iter()) {
                let column_is_key = keys.contains(col);
                columns.push(Column::new(col.clone(), datatype.clone(), column_is_key));
            }
            let db = Table::new(table.clone(), columns);
            let _ = db.save(relation_directory);
            return Ok(Either::That(format!("Created table '{table}'")))
        },
        Query::DELETE(table , column, filter_condition, field_value) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let mut db = load_database(&file_path)?;
            let number_of_rows_deleted = db.delete_rows(column, filter_condition, field_value)?;
            let _ = db.save(relation_directory)?;
            return Ok(Either::That(format!("deleted {} row(s)", number_of_rows_deleted)));
        },
        Query::FILTER(table , column, filter_condition, field_value) => {
            let file_path = format!("{}/db_{table}.bin", &relation_directory);
            let mut db = load_database(&file_path)?;

            let filtered_table = db.select_rows(&column, filter_condition, field_value)?; 
            return Ok(Either::This(filtered_table))
        },
    }

    Err( DBError::ActionNotImplemented("execute_query".to_owned()) )
}


/// used exclusively for query execution, so that I can return a 
/// "number of rows affected" statement or the table
pub enum Either<X, Y> {
    This(X),
    That(Y),
}