use crate::debugger_command::DebuggerCommand;
use crate::inferior::{Inferior, Status};
use nix::sys::signal::Signal;
use rustyline::error::ReadlineError;
use rustyline::Editor;

pub struct Debugger {
    target: String,
    history_path: String,
    readline: Editor<()>,
    inferior: Option<Inferior>,
}

impl Debugger {
    /// Initializes the debugger.
    pub fn new(target: &str) -> Debugger {
        // TODO (milestone 3): initialize the DwarfData

        let history_path = format!("{}/.deet_history", std::env::var("HOME").unwrap());
        let mut readline = Editor::<()>::new();
        // Attempt to load history from ~/.deet_history if it exists
        let _ = readline.load_history(&history_path);

        Debugger {
            target: target.to_string(),
            history_path,
            readline,
            inferior: None,
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.get_next_command() {
                DebuggerCommand::Run(args) => {
                    if let Some(inferior) = Inferior::new(&self.target, &args) {
                        // Create the inferior
                        self.inferior = Some(inferior);
                        // TODO (milestone 1): make the inferior run
                        // You may use self.inferior.as_mut().unwrap() to get a mutable reference
                        // to the Inferior object
                        self.continue_process();
                    } else {
                        println!("Error starting subprocess");
                    }
                }
                DebuggerCommand::Cont => {
                    self.continue_process();
                },
                DebuggerCommand::Quit => {
                    self.process_quit();
                    return;
                }
            }
        }
    }

    /// This function prompts the user to enter a command, and continues re-prompting until the user
    /// enters a valid command. It uses DebuggerCommand::from_tokens to do the command parsing.
    ///
    /// You don't need to read, understand, or modify this function.
    fn get_next_command(&mut self) -> DebuggerCommand {
        loop {
            // Print prompt and get next line of user input
            match self.readline.readline("(deet) ") {
                Err(ReadlineError::Interrupted) => {
                    // User pressed ctrl+c. We're going to ignore it
                    println!("Type \"quit\" to exit");
                }
                Err(ReadlineError::Eof) => {
                    // User pressed ctrl+d, which is the equivalent of "quit" for our purposes
                    return DebuggerCommand::Quit;
                }
                Err(err) => {
                    panic!("Unexpected I/O error: {:?}", err);
                }
                Ok(line) => {
                    if line.trim().len() == 0 {
                        continue;
                    }
                    self.readline.add_history_entry(line.as_str());
                    if let Err(err) = self.readline.save_history(&self.history_path) {
                        println!(
                            "Warning: failed to save history file at {}: {}",
                            self.history_path, err
                        );
                    }
                    let tokens: Vec<&str> = line.split_whitespace().collect();
                    if let Some(cmd) = DebuggerCommand::from_tokens(&tokens) {
                        return cmd;
                    } else {
                        println!("Unrecognized command.");
                    }
                }
            }
        }
    }

    fn process_stopped(&mut self, sig: Signal, pc: usize) {
        println!("Child stopped (SIG={}, pc={})", sig.to_string(), pc);
    }

    fn process_exit(&mut self, exit_code: i32) {
        self.inferior = None;
        println!("Child process exited ({})", exit_code);
    }

    fn process_unexpected_result(&mut self, r: Result<Status, nix::Error>) {
        println!("Unexpected result ({:?})", r)
    }

    fn continue_process(&mut self) {
        match self.inferior.as_mut(){
            Some(inf) => {
                match inf.cont(){
                    Ok(Status::Stopped(sig, pc)) => {
                        self.process_stopped(sig, pc)
                    }, 
                    Ok(Status::Exited(code)) => {
                        self.process_exit(code);
                    },
                    other => {
                        self.process_unexpected_result(other);
                    }
                }
            }, 
            None => {
                println!("invalid command, no existing process");
            }
        }
    }

    fn process_quit(&mut self){
        if let Some(inf) = self.inferior.as_mut(){
            match inf.kill(){
                Ok(Status::Killed) => {
                    println!("Child process killed");
                }, 
                Ok(s) => {
                    println!("Unexpected status returned {:?}", s);
                }, 
                Err(e) => {
                    println!("Failed to kill child process, got {:?}", e);
                }
            }
            self.inferior = None;
        }
    }
}
