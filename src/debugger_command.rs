use crate::helper::parse_address;

pub enum DebuggerCommand {
    Quit,
    Run(Vec<String>),
    Cont,
    Backtrace,
    Break(u64),
}

impl DebuggerCommand {
    pub fn from_tokens(tokens: &Vec<&str>) -> Option<DebuggerCommand> {
        match tokens[0] {
            "q" | "quit" => Some(DebuggerCommand::Quit),
            "r" | "run" => {
                let args = tokens[1..].to_vec();
                Some(DebuggerCommand::Run(
                    args.iter().map(|s| s.to_string()).collect(),
                ))
            }
            "c" | "cont" | "countinue" => Some(DebuggerCommand::Cont),
            "b" | "back" | "backtrace" => Some(DebuggerCommand::Backtrace),
            "break" => {
                // usage: break 0x1234
                if let Some(bk) = parse_address(tokens.get(1).unwrap()) {
                    Some(DebuggerCommand::Break(bk))
                } else {
                    None
                }
            }
            // Default case:
            _ => None,
        }
    }
}
