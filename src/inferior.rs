use nix::sys::ptrace;
use nix::sys::signal;
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::io;
use std::os::unix::process::CommandExt;
use std::process::Child;
use std::process::Command;

use crate::helper;

#[derive(Debug)]
pub enum Status {
    // Init,
    /// Indicates inferior stopped. Contains the signal that stopped the process, as well as the
    /// current instruction pointer that it is stopped at.
    Stopped(signal::Signal, usize),

    /// Indicates inferior exited normally. Contains the exit status code.
    Exited(i32),

    /// Indicates the inferior exited due to a signal. Contains the signal that killed the
    /// process.
    Signaled(signal::Signal),

    Killed,
}

/// This function calls ptrace with PTRACE_TRACEME to enable debugging on a process. You should use
/// pre_exec with Command to call this in the child process.
fn child_traceme() -> Result<(), std::io::Error> {
    ptrace::traceme().or(Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "ptrace TRACEME failed",
    )))
}

pub type Breakpoint = usize;

pub struct Inferior {
    child: Child,
    bks: HashMap<Breakpoint, Option<u8>>,
}

impl Inferior {
    /// Attempts to start a new inferior process. Returns Some(Inferior) if successful, or None if
    /// an error is encountered.
    pub fn new(target: &str, args: &Vec<String>, bks: Vec<Breakpoint>) -> Option<Inferior> {
        println!("target={:?}, args={:?}", target, args);
        let mut cmd = Command::new(target);
        cmd.args(args);

        unsafe {
            cmd.pre_exec(|| child_traceme());
        }

        match cmd.spawn() {
            Ok(child) => {
                let mut inf = Inferior {
                    child,
                    bks: HashMap::with_capacity(bks.len()),
                };

                for bk in bks {
                    if let Err(err) = inf.add_breakpoint(bk) {
                        println!("add bk in {}, got {:?}", bk, err);
                        std::process::exit(-1);
                    }
                    println!("add bk in {}", bk);
                }
                Some(inf)
            }
            Err(e) => {
                println!("failed to run process, got err:{}", e);
                None
            }
        }
    }

    /// Returns the pid of this inferior.
    pub fn pid(&self) -> Pid {
        nix::unistd::Pid::from_raw(self.child.id() as i32)
    }

    /// Calls waitpid on this inferior and returns a Status to indicate the state of the process
    /// after the waitpid call.
    pub fn wait(&self, options: Option<WaitPidFlag>) -> Result<Status, nix::Error> {
        Ok(match waitpid(self.pid(), options)? {
            WaitStatus::Exited(_pid, exit_code) => Status::Exited(exit_code),
            WaitStatus::Signaled(_pid, signal, _core_dumped) => Status::Signaled(signal),
            WaitStatus::Stopped(_pid, signal) => {
                let regs = ptrace::getregs(self.pid())?;
                Status::Stopped(signal, regs.rip as usize)
            }
            other => panic!("waitpid returned unexpected status: {:?}", other),
        })
    }

    pub fn cont(&mut self) -> Result<Status, nix::Error> {
        let _ = ptrace::cont(self.pid(), None)?;
        self.wait(None)
    }

    pub fn kill(&mut self) -> Result<Status, io::Error> {
        self.child.kill().map(|_| return Status::Killed)
    }

    pub fn add_breakpoint(&mut self, rip: usize) -> Result<(), nix::Error> {
        if let Some(_) = self.bks.get(&rip) {
            return Ok(());
        }

        let interrupt: u8 = 0xcc;
        let instruction: u8 = helper::write_byte(self.pid(), rip as u64, interrupt)?;
        self.bks.insert(rip, Some(instruction));

        Ok(())
    }

    pub fn continue_from_breakpoint(&mut self, current_rip: usize) -> Result<(), nix::Error> {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub struct RegisterValue {
    pub rip: usize,
    // rbp: usize,
    pub rsp: usize,
    pub rbp: usize,
}
