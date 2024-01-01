use crate::debugger_command::DebuggerCommand;
use crate::dwarf_data::{DwarfData, Error as DwarfError};
use crate::inferior::{Inferior, RegisterValue, Status};
use gimli::DebugInfo;
use nix::sys::ptrace::{self, AddressType};
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use rustyline::error::ReadlineError;
use rustyline::Editor;

pub struct DebugDataWrapper {
    debug_data: DwarfData,
    pid: Pid,
    reg: RegisterValue,
}

pub struct Debugger {
    target: String,
    history_path: String,
    readline: Editor<()>,
    inferior: Option<Inferior>,
    debug_data: Option<DebugDataWrapper>,
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
            debug_data: None,
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
                }
                DebuggerCommand::Backtrace => {
                    self.process_backtrace();
                }
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
        println!("Child stopped (SIG={}, pc={:#x})", sig.to_string(), pc);
    }

    fn process_exit(&mut self, exit_code: i32) {
        self.inferior = None;
        println!("Child process exited ({})", exit_code);
    }

    fn process_unexpected_result(&mut self, r: Result<Status, nix::Error>) {
        println!("Unexpected result ({:?})", r)
    }

    fn continue_process(&mut self) {
        match self.inferior.as_mut() {
            Some(inf) => match inf.cont() {
                Ok(Status::Stopped(sig, pc)) => self.process_stopped(sig, pc),
                Ok(Status::Exited(code)) => {
                    self.process_exit(code);
                }
                other => {
                    self.process_unexpected_result(other);
                }
            },
            None => {
                self.process_no_inferior();
            }
        }
    }

    fn process_no_inferior(&self) {
        println!("invalid command, no existing process");
    }

    fn process_quit(&mut self) {
        match self.inferior.as_mut() {
            Some(inf) => {
                match inf.kill() {
                    Ok(Status::Killed) => {
                        println!("Child process killed");
                    }
                    Ok(s) => {
                        println!("Unexpected status returned {:?}", s);
                    }
                    Err(e) => {
                        println!("Failed to kill child process, got {:?}", e);
                    }
                }
                self.inferior = None;
            }
            None => {}
        }
    }

    fn process_backtrace(&mut self) {
        match self.inferior.as_mut() {
            Some(inf) => match inf.print_backtrace() {
                Ok(reg) => {
                    if let None = self.debug_data {
                        let target = &self.target;
                        let debug_data = match DwarfData::from_file(target) {
                            Ok(val) => val,
                            Err(DwarfError::ErrorOpeningFile) => {
                                println!("Could not open file {}", target);
                                std::process::exit(1);
                            }
                            Err(DwarfError::DwarfFormatError(err)) => {
                                println!(
                                    "Could not load debugging symbols from {}: {:?}",
                                    target, err
                                );
                                std::process::exit(1);
                            }
                        };

                        let pid = inf.pid();
                        self.debug_data = Some(DebugDataWrapper {
                            debug_data,
                            pid,
                            reg,
                        });
                    }

                    if let Some(w) = self.debug_data.as_ref() {
                        let reg = ptrace::getregs(w.pid).unwrap();
                        let mut rip = reg.rip;
                        let mut rbp = reg.rbp;
                        let mut stack_info: Vec<String> = Vec::with_capacity(64);

                        loop {
                            // get func name
                            // get source name
                            let (s, func_name) = Debugger::get_current_stack_info(w, rip as usize);
                            stack_info.push(s);
                            if func_name == "main" {
                                break;
                            }

                            rip = Debugger::read_from_addr(w.pid, (rbp + 8) as ptrace::AddressType)
                                as u64;
                            rbp =
                                Debugger::read_from_addr(w.pid, rbp as ptrace::AddressType) as u64;
                        }

                        if stack_info.len() > 0 {
                            for row in stack_info {
                                println!("{}", row);
                            }
                        }
                    }
                }
                other => {
                    println!("Unexpected rip value {:?}", other);
                }
            },
            None => self.process_no_inferior(),
        }
    }

    fn get_current_stack_info(w: &DebugDataWrapper, rip: usize) -> (String, String) {
        let source_info = match w.debug_data.get_line_from_addr(rip) {
            Some(line) => line,
            None => {
                println!("invalid $rip value, source code not found");
                std::process::exit(-1);
            }
        };

        let func_name = match w.debug_data.get_function_from_addr(rip) {
            Some(name) => name,
            None => {
                println!("invalid $rip value, function name not found");
                std::process::exit(-1);
            }
        };

        (
            format!(
                "{} ({}:{})",
                func_name, source_info.file, source_info.number
            ),
            func_name,
        )
    }

    fn read_from_addr(pid: Pid, addr: AddressType) -> usize {
        ptrace::read(pid, addr).unwrap() as usize
    }
}
