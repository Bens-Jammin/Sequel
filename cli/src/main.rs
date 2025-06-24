use std::io::{self, Write};

use sequel::{backend::table, table::all_tables_for, ColumnType, Table};
use clap::{Parser, Subcommand};

// cargo install --path .

#[derive(Parser)]
#[command(name = "sequel")]
#[command(about = "a CLI for easy access to the sequel database")]
struct CLI {
    /// Login to a pre-existing user
    #[arg(long)]
    Login: bool,

    /// Signup as a new user
    #[arg(long)]
    Signup: bool

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
    Grab {
        /// Database name to grab
        #[arg(short, long)]
        database: String,
        /// Number of rows to return
        #[arg(short, long, default_value = "25")]
        window: u32,
    },
    /// List available databases
    List,
    /// Show current user info
    Whoami,
    /// Logout
    Logout,
    /// Exit the CLI
    Exit
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

}


fn main() {

    let cli = CLI::parse();
    
    if cli.Login {
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
    } else if cli.Signup {
        println!("!! NOT IMPLEMENTED YET !!");
    }

}

fn interact(mut state: AppState) {
    println!("Welcome to the Sequel database CLI!");
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
                    "exit" | "quit" => { break; }
                    _ => { }
                }

                let args: Vec<&str> = input.split_whitespace().collect();
                if args.is_empty() { continue; }

                let mut full_args = vec!["sequel"];
                full_args.extend(args);
                match InteractiveCLI::try_parse_from(full_args) {
                    Ok(cli) => {
                        if !handle(cli.command, &mut state) { break; }
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
        InteractiveCommand::Grab { database, window } => {
            guarantee_login!(state);
            
            let _db = Table::load( &state.user().unwrap().username, &database ).unwrap();
            println!("!TODO! : Need to implement database retrieval!");
            false
        },
        InteractiveCommand::List => {
            guarantee_login!(state);
            

            let result = String::from("- ") + &all_tables_for(&state.user().unwrap().username).join("\n- ");
            println!("{}", result);

            // println!("!TODO! : Need to implement database retrieval for list command! (maybe throw in sysconfig for admin too?");
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
        InteractiveCommand::Logout => {
            if state.is_logged_in() {
                state.logout();
                println!("Logged out successfully");
            } else {
                println!("Not logged in");
            }
            false
        },
        InteractiveCommand::Exit => { println!("Exiting..."); true },
    }
}


