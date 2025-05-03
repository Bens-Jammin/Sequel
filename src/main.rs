
use sequel::backend::table::mainmem::table::Table;
use sequel::backend::display::cli::view;

pub fn main() {
    let dir = "C:/Programming/TestData/sequel";
    let t = Table::from_csv("C:/Users/benem/Downloads/csv71541.csv", dir).unwrap();

    println!("done parsing table!");
    view( &t.name);

    // when making a table, add it to some c:/users/.../appdata/roaming/sequel folder (which is to be created if it doesn't exist)
    // then that can be called every time because we will know the path every time and no major path
    // manipulation will take place 
    /* to get appdata path:
        let path = std::env::var("APPDATA").unwrap();
        let data_dir = format!("{}/MyRustApp", path);
    */
}