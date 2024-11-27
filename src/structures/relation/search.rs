use chrono::DateTime;

use crate::structures::{column::FieldValue, db_err::DBError, filter::{FilterCondition, FilterConditionValue}};



pub fn non_index_row_matches_search_critieria(row_value: &FieldValue, search_criteria: &FilterCondition) 
-> Result<bool, DBError> {

    fn check_against_condition(
        condition_value: &FilterConditionValue, 
        op: fn(&FilterConditionValue, f64) -> bool 
    ) 
    -> Result<bool, DBError> {
        match condition_value {
            FilterConditionValue::Number(condition_target) => { Ok( op( condition_value, *condition_target ) ) }
            _ => return Err(DBError::MisMatchConditionDataType(FilterConditionValue::Number(0.0), condition_value.clone()))
        }

    } 

    match &search_criteria {
        // check if the condition is a relational operator (i.e. >, >=, ==, !=, <, <=)
        FilterCondition::LessThan(condition_value) =>
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() < v2),
        FilterCondition::LessThanOrEqualTo(condition_value) =>
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() <= v2),
        FilterCondition::GreaterThan(condition_value) =>
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() > v2),
        FilterCondition::GreaterThanOrEqualTo(condition_value) =>
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() >= v2),
        FilterCondition::Equal(condition_value) => 
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() == v2),
        FilterCondition::NotEqual(condition_value) =>
            check_against_condition(condition_value, |v1, v2| v1.number().unwrap() != v2),
        FilterCondition::NumberBetween(condition_value) => {
            // make sure the target value is a range so we can see if the cell value is in a range
            match &condition_value { 
                FilterConditionValue::NumberRange(lower_bound, upper_bound) => {
                    Ok(FieldValue::Number(*lower_bound).is_less_than(row_value)? 
                    && FieldValue::Number(*upper_bound).is_greater_than(row_value)?)
                },
                    _ => return Err(DBError::MisMatchConditionDataType(
                    FilterConditionValue::DateRange(DateTime::default(), DateTime::default()), condition_value.clone()
                )) 
            }
        },
        FilterCondition::DateBetween(condition_value) => {
            // make sure the target value is a range so we can see if the cell value is in a range
            match &condition_value { 
                FilterConditionValue::DateRange(lower_bound, upper_bound) => {
                    Ok(FieldValue::Date(*lower_bound).is_less_than(row_value)? 
                    && FieldValue::Date(*upper_bound).is_greater_than(row_value)?)
                },
                    _ => return Err(DBError::MisMatchConditionDataType(
                    FilterConditionValue::NumberRange(0.0, 0.0), condition_value.clone()
                )) 
            }
        },
        FilterCondition::True                 => Ok( row_value.eq( &FieldValue::Boolean(true)  )),
        FilterCondition::False                => Ok( row_value.eq( &FieldValue::Boolean(false) )),
        FilterCondition::Null                 => Ok( row_value.eq(&FieldValue::Null)),
        FilterCondition::NotNull              => Ok(!row_value.eq(&FieldValue::Null)),
    }
}
