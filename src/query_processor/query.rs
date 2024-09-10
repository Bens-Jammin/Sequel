use crate::structures::{column::{parse_str, DataType}, modify_where::FilterCondition, sort_method::SortCondition};

#[derive(Debug)]
pub enum Query {
    /// SELECT (col1, col2, ..., coln) FROM (table)
    SELECT(Vec<String>, String),

    /// INSERT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
    INSERT(Vec<String>, String, Vec<String>),

    /// EDIT (val1, val2, ..., valn) INTO (table) (col1, col2, ..., coln)
    EDIT(Vec<String>, String, Vec<String>),

    /// REMOVE FROM (table) WHERE (condition)
    // FIXME: DELETE(String, FilterCondition),

    /// SORT (table) ON (sort_condition) 
    SORT(String, SortCondition),

    /// FILTER (table) ON (filter_condition)
    // FIXME: FILTER(String, FilterCondition),

    /// INDEX (table) (column)
    INDEX(String, String),

    // CREATE (table_name) COLUMNS (col_name1:data_type1, etc) KEYS (col_name_1, etc)
    CREATE(String, Vec<String>, Vec<DataType>, Vec<String>)
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
/// SORT `(table)` ON `(sort_condition)` <br>
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
        // NOT IMPLEMENTED YET!
        return None;

        // REMOVE FROM (table) WHERE (condition)
        // if let (Some(from_index), Some(where_index)) = ( 
        //     parts.iter().position(|&s| s.to_lowercase() == "from"), 
        //     parts.iter().position(|&s| s.to_lowercase() == "where")
        // ) {
        //     let table = parts[from_index + 1].trim_matches(|c| c == '(' || c == ')').to_string();
        //     let condition = FilterCondition::parse_str( parts[where_index + 1] );
        //     println!("{:?}", condition);
        //     match condition {
        //         None => return None,
        //         Some(_) => (),
        //     }
        //     return Some(Query::DELETE(table, condition.unwrap()));
        // }
    } else if main_query_command.starts_with("sort") {
        // SORT (table) ON (sort_condition)
        if let Some(on_index) = parts.iter().position(|&s| s.to_lowercase() == "on") {
            let table = parts[1].trim_matches(|c| c == '(' || c == ')').to_string();
            let sort_condition = SortCondition::parse_str( parts[on_index + 1] );
            println!("{:?}", sort_condition);
            match sort_condition {
                None => return None,
                Some(_) => (),
            } 

            return Some(Query::SORT(table, sort_condition.unwrap()));
        }
    } else if main_query_command.starts_with("filter") {
        // FILTER (table) ON (filter_condition)
        // NOT IMPLEMENTED YET! FIX `FILTER_CONDITION`
        return None;
        // if let Some(on_index) = parts.iter().position(|&s| s.to_lowercase() == "on") {
        //     let table = parts[1].trim_matches(|c| c == '(' || c == ')').to_string();
        //     let filter_condition = FilterCondition::parse_str( parts[on_index + 1] );
        //     println!("{:?}", filter_condition);
        //     match filter_condition {
        //         None => return None,
        //         Some(_) => (),
        //     }
        //     return Some(Query::FILTER(table, filter_condition.unwrap()));
        // }
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

pub fn test_parse_query() {
    let mut passed_tests = 0;
    let mut total_tests = 0;
    let mut error_messages: Vec<String> = Vec::new();

    // === PASSED TESTS ===

    // Test 1: Valid SELECT query
    total_tests += 1;
    let select_query = "SELECT (name, age) FROM (people)".to_string();
    match parse_query(select_query) {
        Some(Query::SELECT(cols, table)) => {
            if cols == vec!["name".to_string(), "age".to_string()] && table == "people".to_string() {
                passed_tests += 1;
            } else {
                error_messages.push(format!(
                    "Test 1 Failed: Incorrect parsing result: cols={:?}, table={}",
                    cols, table
                ));
            }
        }
        _ => error_messages.push("Test 1 Failed: Query parsing failed or returned incorrect variant.".to_string()),
    }

    // Test 2: Valid INSERT query
    total_tests += 1;
    let insert_query = "INSERT (John, 30) INTO (friends) (name, age)".to_string();
    match parse_query(insert_query) {
        Some(Query::INSERT(vals, table, cols)) => {
            if vals == vec!["John".to_string(), "30".to_string()] && table == "friends".to_string() && cols == vec!["name".to_string(), "age".to_string()] {
                passed_tests += 1;
            } else {
                error_messages.push(format!(
                    "Test 2 Failed: Incorrect parsing result: vals={:?}, table={}, cols={:?}",
                    vals, table, cols
                ));
            }
        }
        _ => error_messages.push("Test 2 Failed: Query parsing failed or returned incorrect variant.".to_string()),
    }

    // Test 3: Valid DELETE query
    // total_tests += 1;
    // let delete_query = "REMOVE FROM (users) WHERE (age = 20)".to_string();
    // match parse_query(delete_query.clone()) {
    //     Some(Query::DELETE(table, _)) => {
    //         if table == "users".to_string() {
    //             passed_tests += 1;
    //         } else {
    //             error_messages.push(format!(
    //                 "Test 3 (remove) Failed: Incorrect parsing result: table={}",
    //                 table
    //             ));
    //         }
    //     }
    //     _ => error_messages.push(format!(
    //         "Test 3 Failed: Query parsing failed or returned incorrect variant: {:?}",
    //         parse_query(delete_query)
    //     )),
    // }

    // Test 4: Valid SORT query
    total_tests += 1;
    let sort_query = "SORT (people) ON (numeric_ascending)".to_string();
    match parse_query(sort_query.clone()) {
        Some(Query::SORT(table, _)) => {
            if table == "people".to_string() {
                passed_tests += 1;
            } else {
                error_messages.push(format!(
                    "Test 4 (sort) Failed: Incorrect parsing result: table={}",
                    table
                ));
            }
        }
        _ => error_messages.push(format!(
            "Test 4 Failed: Query parsing failed or returned incorrect variant: {:?}",
            parse_query(sort_query)
        )),
    }

     // Test 5: Valid FILTER query
    // total_tests += 1;
    // let sort_query = "FILTER (people) ON (date_descending)".to_string();
    // match parse_query(sort_query.clone()) {
    //     Some(Query::FILTER(table, _)) => {
    //         if table == "people".to_string() {
    //             passed_tests += 1;
    //         } else {
    //             error_messages.push(format!(
    //                 "Test 5 (filter) Failed: Incorrect parsing result: table={}",
    //                 table
    //             ));
    //         }
    //     }
    //     _ => error_messages.push(format!(
    //         "Test 5 Failed: Query parsing failed or returned incorrect variant: {:?}",
    //         parse_query(sort_query)
    //     )),
    // }

    // test 6: CREATE query
    total_tests += 1;
    let create_query = "CREATE (users) COLUMNS (id:Number, name:String, age:Number) KEYS (id)".to_string();
    match parse_query(create_query.clone()) {
        Some(Query::CREATE(table, columns, data_types, keys)) => {
            if table == "users" && columns == vec!["id".to_string(), "name".to_string(), "age".to_string()] && keys == vec!["id".to_string()] {
                // Assume FieldValue has a reasonable debug format for comparison
                let expected_values = vec![DataType::Number, DataType::String, DataType::Number];
                if data_types == expected_values {
                    passed_tests += 1;
                } else {
                    error_messages.push(format!(
                        "Test 6 (create) Failed: Incorrect field values: {:?}",
                        data_types
                    ));
                }
            } else {
                error_messages.push(format!(
                    "Test 6 (create) Failed: Incorrect parsing result: table={}, columns={:?}, keys={:?}",
                    table, columns, keys
                ));
            }
        }
        _ => error_messages.push(format!(
            "Test 6 Failed: Query parsing failed or returned incorrect variant: {:?}",
            parse_query(create_query)
        )),
    }

    // Test 7: Invalid query
    total_tests += 1;
    let invalid_query = "INVALID QUERY".to_string();
    match parse_query(invalid_query) {
        None => passed_tests += 1, // Expect None for invalid query
        _ => error_messages.push("Test 7 Failed: Invalid query should have returned None.".to_string()),
    }

    // === RESULTS ===
    println!(" == PARSE QUERY TESTS == ");
    println!("Tests done: {}", total_tests);
    println!("Passed tests: {}", passed_tests);
    println!("Error messages: \n{:#?}", error_messages);
}
