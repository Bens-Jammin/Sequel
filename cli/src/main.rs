use std::io::{self, Write};

// use sequel;
use clap::{Parser, Subcommand};

// cargo install --path .

#[derive(Parser)]
#[command(name = "s2l")]
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
#[command(name = "s2l")]
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
            format!("{}@s2l>{} ", user.username, mode)
        } else { "s2l> ".to_string() };
          print!("{prompt}");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let input = input.trim();
                if input.is_empty() { continue; }

                match input {
                    "help" => {
                        if let Err(e) = InteractiveCLI::try_parse_from(vec!["s2l", "--help"]) {
                            println!("{e}")
                        }
                        continue;
                    }
                    "exit" | "quit" => { break; }
                    _ => { }
                }

                let args: Vec<&str> = input.split_whitespace().collect();
                if args.is_empty() { continue; }

                let mut full_args = vec!["s2l"];
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


fn handle(cmd: InteractiveCommand , state: &mut AppState) -> bool {
    match cmd {
        InteractiveCommand::Grab { database, window } => {
            if !state.is_logged_in() {
                eprintln!("Error: Must be signed in to grab a database."); return false;
            }
            println!("!TODO! : Need to implement database retrieval!");
            false
        },
        InteractiveCommand::List => {
            if !state.is_logged_in() {
                eprintln!("Error: You must be logged in to list databases"); return false;
            }

            println!("!TODO! : Need to implement database retrieval for list command! (maybe throw in sysconfig for admin too?");
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
                let username = state.user().unwrap().username.clone();
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