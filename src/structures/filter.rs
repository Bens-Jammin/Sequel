use core::fmt;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

#[derive(Debug, Clone)]
pub enum FilterConditionValue {
    String(String),
    Number(f64),
    Date(DateTime<Utc>),
    
    /// ## Note:
    /// inclusive range
    NumberRange(f64, f64),

    /// ## Note:
    /// inclusive range
    DateRange(DateTime<Utc>, DateTime<Utc>),
}

impl FilterConditionValue {
    pub fn name(&self) -> String {
        match self {
            FilterConditionValue::String(_) => String::from("String"),
            FilterConditionValue::Number(_) => String::from("Number"),
            FilterConditionValue::Date(_) => String::from("Date"),
            FilterConditionValue::NumberRange(_, _) => String::from("Number range"),
            FilterConditionValue::DateRange(_, _) => String::from("Date range"),
        }
    }

    pub fn str(&self) -> Option<String> {
        match self {
            FilterConditionValue::String(v) => return Some(v.clone()),
            _ => return None
        }
    }

    pub fn number(&self) -> Option<f64> {
        match self {
            FilterConditionValue::Number(v) => return Some(v.clone()),
            _ => return None
        }
    }
    pub fn date(&self) -> Option<DateTime<Utc>> {
        match self {
            FilterConditionValue::Date(v) => return Some(v.clone()),
            _ => return None
        }
    }

    pub fn number_range(&self) -> Option<(f64, f64)> {
        match self {
            FilterConditionValue::NumberRange(v1, v2) => return Some( (v1.clone(), v2.clone()) ),
            _ => return None
        }
    }

    pub fn date_range(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        match self {
            FilterConditionValue::DateRange(v1, v2) => return Some( (v1.clone(), v2.clone() ) ),
            _ => return None
        }
    }




}



#[derive(Debug, Clone)]
pub enum FilterCondition {
    LessThan(FilterConditionValue),
    LessThanOrEqualTo(FilterConditionValue),
    GreaterThan(FilterConditionValue),
    GreaterThanOrEqualTo(FilterConditionValue),
    Equal(FilterConditionValue),
    NotEqual(FilterConditionValue),
    True,
    False,
    Null,
    NotNull,

    /// an inclusive range between two 64 bit floats.
    NumberBetween(FilterConditionValue), 
    
    /// an inclusive range between two dates.
    DateBetween(FilterConditionValue),

}

impl FilterCondition {
    pub fn parse_str(input: &str) -> Option<FilterCondition> {
        
        println!("parsing `{:?}` as a filter condition...", input);
        let condition_components: Vec<String> = input
            .trim()
            .to_lowercase()
            .split_whitespace()
            .map(|s| str::to_string(s))
            .collect();

        println!("conditioncomponents[0] = {}", &condition_components[0]);

        // check if the filter condition is an inequality, equals, or not equals
        let valid_relational_operators = vec!["<", "<=", "=", "!=", ">=", ">"];
        if valid_relational_operators.contains(&condition_components[0].as_str()) {
            let condition_value = condition_components[1].parse::<f64>().unwrap_or(-1.0);

            match condition_components[0].trim() {
                "<=" => return Some(FilterCondition::LessThanOrEqualTo(FilterConditionValue::Number(condition_value))),
                "<" => return Some(FilterCondition::LessThan(FilterConditionValue::Number(condition_value))),
                "=" => return Some(FilterCondition::Equal(FilterConditionValue::Number(condition_value))),
                "!=" => return Some(FilterCondition::NotEqual(FilterConditionValue::Number(condition_value))),
                ">" => return Some(FilterCondition::GreaterThan(FilterConditionValue::Number(condition_value))),
                ">=" => return Some(FilterCondition::GreaterThanOrEqualTo(FilterConditionValue::Number(condition_value))),
                _ => ()
            }
        }
        
        // check if condition is a range
        if condition_components[0] == "between" {
            match condition_components[1].as_str() {
                "dates" => {
                    let lower_bound = parse_into_date(&condition_components[2]).unwrap();
                    let upper_bound = parse_into_date(&condition_components[3]).unwrap();
                    return Some(FilterCondition::DateBetween(FilterConditionValue::DateRange(lower_bound, upper_bound)))
                }
                "numbers" => {
                    let lower_bound = condition_components[1].parse::<f64>().unwrap();
                    let upper_bound = condition_components[2].parse::<f64>().unwrap();
                    return Some( FilterCondition::NumberBetween(FilterConditionValue::NumberRange(lower_bound, upper_bound)))
                }
                _ => (),
            }
        }

        // check if it's a boolean check
        match input.trim().to_lowercase().as_str() {
            "true" => Some(FilterCondition::True),
            "false" => Some(FilterCondition::False),
            _ => None  // Otherwise, no valid filter found
        }
    }
}


fn parse_into_date(str: &str) -> Option<DateTime<Utc>> {

    let separator = if str.contains("-") {"-"} else {"/"};
    
    let date_format = format!("%Y{}%m{}%d", separator, separator);

    // check if a timestamp is included or not
    if str.contains(":") {
        let datetime_format = format!("{} %H:%M:%S", date_format);
        let datetime = DateTime::parse_from_str(str, &datetime_format).unwrap();
        let r = datetime.with_timezone(&Utc);
        return Some(r);
    }
    // assume timestamp is 0:00:00
    let date: NaiveDate = NaiveDate::parse_from_str(str, &date_format).unwrap();
    Some(date.and_time(NaiveTime::default()).and_utc())
}


impl fmt::Display for FilterConditionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterConditionValue::String(v) => write!(f, "{v}") ,
            FilterConditionValue::Number(v) => write!(f, "{v}") ,
            FilterConditionValue::Date(v) => write!(f, "{v}") ,
            FilterConditionValue::NumberRange(lb, ub) => write!(f, "[{lb}, {ub}]"),
            FilterConditionValue::DateRange(lb, ub) => write!(f, "[{lb}, {ub}]"),
        }
    }
}

impl fmt::Display for FilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterCondition::True                   => write!(f, "Is True"),
            FilterCondition::False                  => write!(f, "Is False"),
            FilterCondition::Null                   => write!(f, "is Null"),
            FilterCondition::NotNull                => write!(f, "is Not Null"),
            FilterCondition::LessThan(v)             => write!(f, "< {v}"),
            FilterCondition::LessThanOrEqualTo(v)    => write!(f, "<= {v}"),
            FilterCondition::GreaterThan(v)          => write!(f, "> {v}"),
            FilterCondition::GreaterThanOrEqualTo(v) => write!(f, ">= {v}"),
            FilterCondition::Equal(v)                => write!(f, "= {v}"),
            FilterCondition::NotEqual(v)             => write!(f, "!= {v}"),
            FilterCondition::DateBetween(v)          => write!(f, "In the inclusive range {v}"), 
            FilterCondition::NumberBetween(v)        => write!(f, "In the inclusive range {v}"), 
        }
    }
}