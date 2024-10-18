#[derive(Debug)]
pub enum SortCondition {
    NumericAscending,
    NumericDescending,
    AlphaAscending,
    AlphaDescending,
    DateAscending,
    DateDescending
}

impl SortCondition {
    pub fn parse_str(str: &str) -> Option<SortCondition> {
        let cleaned_string = str.trim().to_lowercase();
        let cleaned_string = cleaned_string.replace(")", "");
        let cleaned_string = cleaned_string.replace("(", "");
        return match cleaned_string.as_str() {
            "numeric_ascending" => Some(SortCondition::NumericAscending),
            "numeric_descending" => Some(SortCondition::NumericDescending),
            "alpha_ascending" => Some(SortCondition::AlphaAscending),
            "alpha_descending" => Some(SortCondition::AlphaDescending),
            "date_ascending" => Some(SortCondition::DateAscending),
            "date_descending" => Some(SortCondition::DateDescending),
            _ => None
        }
    }
}