use std::io::{self, Write};

use sequel::{table::all_tables_for, ColumnType, FieldValue, Table};
use clap::{Parser, Subcommand};

// cargo install --path .

#[derive(Parser)]
#[command(name = "sequel")]
#[command(about = "a CLI for easy access to the sequel database")]
struct CLI {
    /// Login to a pre-existing user
    #[arg(long)]
    login: bool,

    /// Signup as a new user
    #[arg(long)]
    signup: bool

}


// Add this new CLI struct for interactive mode
#[derive(Parser)]
#[command(name = "sequel")]
#[command(about = "Available commands:")]
#[command(override_usage = "<COMMAND> <ARGS>")]
struct InteractiveCLI {
    #[command(subcommand)]
    command: InteractiveCommand
}


#[derive(Subcommand)]
enum InteractiveCommand {
    /// Grab a database (requires login)
    #[command(short_flag='g')]
    Grab {
        /// Database name to grab
        #[arg(short, long)]
        table: String,
        /// Number of rows to return
        #[arg(short, long, default_value = "25")]
        window: u32,
    },
    /// List available databases
    List {
        /// Lists all databases for all tables
        #[arg(short, long)]
        all: bool,
        
        /// adds detail to the listed tables (row/col count, indexes, etc)
        #[arg(short, long)]
        verbose: bool
    },
    /// Show current user info
    Whoami,
    /// Creates a new table under the user
    Make {
        #[arg(short, long)]
        name: String,

        columns: String
    },
    /// add a row into the table
    Insert {
        ///Table the row is being inserted into
        #[arg(short, long, value_name="TABLE NAME")]
        table: String,
        
        /// comma separated values for the data you want in the row 
        #[arg(short, long, value_name="ROW DATA")]
        data: String
    },
    /// Logout
    Logout,
    /// Login
    Login,
    /// Exit the CLI
    Quit
}



struct UserSession {
    username: String,
    is_admin: bool
}


struct AppState { user: Option<UserSession> }

impl AppState {
    fn init() -> Self { AppState{user: None} }
    fn login(&mut self, username: String) -> Result<(), String> {
        // if user exists
        if username == "admin" {
            self.user = Some(UserSession {  username,  is_admin: true });
        } else if username == "testuser" {
            self.user = Some(UserSession { username, is_admin: false });
        } else {
            return Err("User does not exist".to_string())
        }
        Ok(())
    }
    fn logout(&mut self) { self.user = None; }
    fn is_logged_in(&self) -> bool { self.user.is_some() }

    fn user(&self) -> Option<&UserSession> { self.user.as_ref() }
    
    /// returns false if the user is not an admin or no one is logged in
    fn isadmin(&self) -> bool { match &self.user { Some(u) => u.is_admin, None => false }  }
}


#[allow(unused)]
fn test_insert() {

    let mut t = Table::init("admin".to_string(), "TestTable".to_string(), vec![
        ("A".to_string(), (ColumnType::STRING , false)),
        ("B".to_string(), (ColumnType::NUMBER , false)),
        ("C".to_string(), (ColumnType::BOOLEAN, false)),
    ]);

    t.insert_row(vec![
        FieldValue::STRING(String::new()),
        FieldValue::NUMBER(100),
        FieldValue::BOOL(true)
    ]);

    t.insert_row(vec![
        FieldValue::STRING(String::new()),
        FieldValue::NUMBER(100),
        FieldValue::BOOL(true)
    ]);

    t.insert_row(vec![
        FieldValue::STRING(String::new()),
        FieldValue::NUMBER(100),
        FieldValue::BOOL(true)
    ]);
    println!("{}",t.as_string(0, 10));
}


fn main() {

    test_insert();
    // cargo run -- --login
    let cli = CLI::parse();

    if cli.login {
        print!("Enter username: ");
        io::stdout().flush().unwrap(); 
        
        let mut username = String::new();
        io::stdin().read_line(&mut username).unwrap();
        let username = username.trim();

        let mut appstate = AppState::init();
            match appstate.login(username.to_string()) {
                Ok(()) => { 
                    println!("logged in, welcome, {username}!");
                    interact(appstate);
                },
                Err(e) => {
                    println!("error logging in!: {e}");
                },
            }
    } else if cli.signup {
        println!("!! NOT IMPLEMENTED YET !!");
    }

}

fn interact(mut state: AppState) {
    println!("Welcome to the Sequel database CLI Version 0.5!");
    loop {
        let prompt = if let Some(user) = state.user() {
            let mode = if user.is_admin { "#" } else { "" };
            format!("{}@sequel>{} ", user.username, mode)
        } else { "sequel> ".to_string() };
          print!("{prompt}");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let input = input.trim();
                if input.is_empty() { continue; }

                match input {
                    "help" => {
                        if let Err(e) = InteractiveCLI::try_parse_from(vec!["sequel", "--help"]) {
                            println!("{e}")
                        }
                        continue;
                    }
                    "quit" => { break; }
                    _ => { }
                }

                let args: Vec<&str> = input.split_whitespace().collect();
                if args.is_empty() { continue; }

                let mut full_args = vec!["sequel"];
                full_args.extend(args);
                match InteractiveCLI::try_parse_from(full_args) {
                    Ok(cli) => {
                        if handle(cli.command, &mut state) { break; }
                    },
                    Err(e) => eprintln!("{e}")
                }
            
            },
            Err(e) => {
                eprintln!("An error occurred!: {e}"); break;
            },
        }
    }
}


macro_rules! guarantee_login {
    ($state: expr) => {
        if !$state.is_logged_in() {
            eprintln!("Error: You must be logged in to access the database!"); return false;
        }
    };
}




fn handle(cmd: InteractiveCommand , state: &mut AppState) -> bool {

    match cmd {
        InteractiveCommand::Grab { table, window } => {
                        guarantee_login!(state);
                
                        let db = Table::load( &state.user().unwrap().username, &table ).unwrap();
                        println!("{}",db.as_string(0, window as usize));
                        false
            },
        InteractiveCommand::List { all, verbose } => {
                guarantee_login!(state);

                // maintain administrative control
                if all && !state.isadmin() { println!("ERROR: 'all' is an admin only flag!"); return false; }
                if verbose && !state.isadmin() { println!("ERROR: 'verbose' is an admin only flag!"); return false; }

                let result = String::from("- ") + &all_tables_for(&state.user().unwrap().username).join("\n- ");
                println!("{}", result);

                false
            },
        InteractiveCommand::Whoami => {
                if let Some(user) = state.user() {
                    let admin_state = if user.is_admin {"(admin)"} else {""};
                    println!("Logged in as: {} {}", user.username, admin_state);
                } else {
                    println!("Not logged in");
                }
                false
            },
        InteractiveCommand::Insert {table, data} => {
                guarantee_login!(state);

                let mut db = Table::load( &state.user().unwrap().username, &table ).unwrap();
            
                // split on comma, remove redundant whitespace
                let values: Vec<&str> = data
                    .split(',')
                    .map(|item| 
                        item.trim()
                    ).collect();
            
                let field_values: Vec<FieldValue> = values.iter().map(|d| FieldValue::parse(d) ).collect();
            

                db.insert_row(field_values);
                println!("Inserted row.");
                false
            },
        InteractiveCommand::Make { name, columns: _ } => {
            guarantee_login!(state);

            // TODO! parse a list of column datapoints from a string !!

            let _ = Table::init(state.user().unwrap().username.clone(), name, vec![] );

            false
        },
        InteractiveCommand::Login => {
                print!("enter username: ");
                let _ = io::stdout().flush();
                let mut username = String::new();
                io::stdin().read_line(&mut username).unwrap();
                let username = username.trim();

                match state.login(username.to_string()) {
                    Ok(()) => { println!("logged in, welcome, {username}!"); },
                    Err(e) => { println!("error logging in!: {e}"); },
                }
                false
        }
        InteractiveCommand::Logout => { state.logout(); false },
        InteractiveCommand::Quit => { println!("Exiting..."); true },
    }

    // TODO: INSERTIONS FUCKED! -- errors after writing once ?
}


