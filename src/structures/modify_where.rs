use core::fmt;

use chrono::{DateTime, Utc};


/// TODO: add to parse_str() the ability to parse notlike, numberbetween, datebetween
#[derive(Debug)]
pub enum FilterCondition {
    LessThan,
    LessThanOrEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    Equal,
    NotEqual,
    Like,
    NotLike,
    /// an inclusive range between two floats
    NumberBetween(f64, f64),
    /// an inclusive range between two dates
    DateBetween(DateTime<Utc>, DateTime<Utc>),
    True,
    False,
 
    // === UNABLE TO IMPLEMENT YET ===
    // Null,
    // NotNull
    //KeyIs(String),   
    //KeyIsNot(String),

}

impl FilterCondition {
    pub fn parse_str(input: &str) -> Option<FilterCondition> {
        let r = match input.trim().to_lowercase().as_str() {
            "<" => Some(FilterCondition::LessThan),
            "<=" => Some(FilterCondition::LessThanOrEqualTo),
            ">" => Some(FilterCondition::GreaterThan),
            ">=" => Some(FilterCondition::GreaterThanOrEqualTo),
            "=" => Some(FilterCondition::Equal),
            "!=" => Some(FilterCondition::NotEqual),
            "like" => Some(FilterCondition::Like),
            "true" => Some(FilterCondition::True),
            "false" => Some(FilterCondition::False),
            _ => None,
            // TODO: add notlike, numberbetween, datebetween
        };
        println!("str = '{}', r = '{:?}'", input, &r);
        r
    }
}



impl fmt::Display for FilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterCondition::LessThan             => write!(f, "LessThan"),
            FilterCondition::LessThanOrEqualTo    => write!(f, "LessThanOrEqualTo"),
            FilterCondition::GreaterThan          => write!(f, "GreaterThan"),
            FilterCondition::GreaterThanOrEqualTo => write!(f, "GreaterThanOrEqualTo"),
            FilterCondition::Equal                => write!(f, "Equal"),
            FilterCondition::NotEqual             => write!(f, "NotEqual"),
            FilterCondition::Like                 => write!(f, "Like"),
            FilterCondition::NotLike              => write!(f, "NotLike"),
            FilterCondition::True                 => write!(f, "IsTrue"),
            FilterCondition::False                => write!(f, "IsFalse"),
            FilterCondition::NumberBetween(a, b) => write!(f, "NumberBetween ({}, {})", a, b),
            FilterCondition::DateBetween(a, b) => write!(f, "DateBetween ({}, {}", a, b),
        }
    }
}