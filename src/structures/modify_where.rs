use core::fmt;

#[derive(Debug, Clone)]
pub enum FilterCondition {
    LessThan,
    LessThanOrEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    Equal,
    NotEqual,
    True,
    False,
    Null,
    NotNull,

    /// an inclusive range between two 64 bit floats.
    NumberBetween, 
    
    /// an inclusive range between two dates.
    DateBetween,

}

impl FilterCondition {
    pub fn parse_str(input: &str) -> Option<FilterCondition> {
        match input.trim().to_lowercase().as_str() {
            "<" => Some(FilterCondition::LessThan),
            "<=" => Some(FilterCondition::LessThanOrEqualTo),
            ">" => Some(FilterCondition::GreaterThan),
            ">=" => Some(FilterCondition::GreaterThanOrEqualTo),
            "=" => Some(FilterCondition::Equal),
            "!=" => Some(FilterCondition::NotEqual),
            "true" => Some(FilterCondition::True),
            "false" => Some(FilterCondition::False),
            "between dates" => Some(FilterCondition::DateBetween),
            "between numbers" => Some(FilterCondition::NumberBetween),

            "not like" => None, 
            _ => None,
        }
    }
}



impl fmt::Display for FilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterCondition::LessThan             => write!(f, "<"),
            FilterCondition::LessThanOrEqualTo    => write!(f, "<="),
            FilterCondition::GreaterThan          => write!(f, ">"),
            FilterCondition::GreaterThanOrEqualTo => write!(f, ">="),
            FilterCondition::Equal                => write!(f, "="),
            FilterCondition::NotEqual             => write!(f, "!="),
            FilterCondition::True                 => write!(f, "Is True"),
            FilterCondition::False                => write!(f, "Is False"),
            FilterCondition::Null                 => write!(f, "Null"),
            FilterCondition::NotNull              => write!(f, "Not Null"),
            FilterCondition::DateBetween          => write!(f, "Between dates"), 
            FilterCondition::NumberBetween        => write!(f, "Between numbers"), 
        }
    }
}