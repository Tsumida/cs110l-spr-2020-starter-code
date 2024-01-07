use crate::debugger_command::DebuggerCommand;
use crate::dwarf_data::{DwarfData, Error as DwarfError};
use crate::inferior::{Breakpoint, Inferior, RegisterValue, Status};
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

    // Breakpoint buffer
    bks: Vec<Breakpoint>,
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
            bks: vec![],
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.get_next_command() {
                DebuggerCommand::Run(args) => {
                    self.inferior = Inferior::new(&self.target, &args, self.bks.clone());
                    if let Some(inf) = self.inferior.as_mut() {
                        let d = Debugger::new_debug_wrapper(&&self.target, inf.pid()).unwrap();
                        d.debug_data.print();
                        self.debug_data = Some(d);

                        match inf.wait(None) {
                            Ok(Status::Stopped(sig, rip)) => {
                                self.process_stopped(
                                    sig,
                                    rip,
                                    self.is_stopped_by_bk(Inferior::get_prev_rip(rip)),
                                );
                            }
                            other => {
                                println!("Unexpected result {:?}", other);
                            }
                        }
                    } else {
                        println!("Error starting subprocess");
                    }
                }
                DebuggerCommand::Break(bk) => {
                    self.process_add_break(bk as usize);
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

    fn process_stopped(&mut self, sig: Signal, rip: usize, print_backtrace: bool) {
        println!(
            "Child stopped (signal {}, rip = {:#x}, pb = {})",
            sig.to_string(),
            rip,
            print_backtrace,
        );

        let pid = self.inferior.as_ref().unwrap().pid();
        let d = Debugger::new_debug_wrapper(&self.target, pid).unwrap();

        match d.debug_data.get_line_from_addr(Inferior::get_prev_rip(rip)) {
            Some(line) => {
                println!("Stopped at {}:{}", line.file, line.number);
            }
            None => {
                println!("Failed to locate source at {:#x}", rip);
                return;
            }
        }

        self.debug_data = Some(d);
    }

    fn process_exit(&mut self, exit_code: i32) {
        self.inferior = None;
        println!("Child process exited ({})", exit_code);
    }

    fn process_unexpected_result(&mut self, r: Result<Status, nix::Error>) {
        println!("Unexpected result ({:?})", r);
        std::process::exit(-1);
    }

    fn continue_process(&mut self) {
        match self.inferior.as_mut() {
            Some(inf) => match inf.cont() {
                Ok(Status::Stopped(sig, rip)) => self.process_stopped(
                    sig,
                    rip,
                    self.is_stopped_by_bk(Inferior::get_prev_rip(rip)),
                ),
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

    fn new_debug_wrapper(target: &str, pid: Pid) -> Result<DebugDataWrapper, nix::Error> {
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

        let reg_val = ptrace::getregs(pid)?;
        Ok(DebugDataWrapper {
            debug_data,
            pid,
            reg: RegisterValue {
                rip: reg_val.rip as usize,
                rsp: reg_val.rsp as usize,
                rbp: reg_val.rbp as usize,
            },
        })
    }

    fn process_backtrace(&mut self) {
        if self.inferior.is_none() {
            self.process_no_inferior();
            return;
        }

        let w = self.debug_data.as_ref().unwrap();
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

            rip = Debugger::read_from_addr(w.pid, (rbp + 8) as ptrace::AddressType) as u64;
            rbp = Debugger::read_from_addr(w.pid, rbp as ptrace::AddressType) as u64;
        }

        if stack_info.len() > 0 {
            for row in stack_info {
                println!("{}", row);
            }
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

    fn process_add_break(&mut self, bk: usize) {
        self.bks.push(bk as usize);
        if let Some(inf) = self.inferior.as_mut() {
            if let Err(err) = inf.add_breakpoint(bk) {
                println!("Add breakpoint {}, got {:?}", bk, err);
            }
        }
        println!("Set breakpoint {} at {}", self.bks.len(), bk);
    }

    fn is_stopped_by_bk(&self, prev_rip: usize) -> bool {
        self.inferior.as_ref().unwrap().is_stopped_by_bk(prev_rip)
    }
}
