use std::mem::size_of;

use nix::{sys::ptrace, unistd::Pid};

pub fn align_addr_to_word(addr: u64) -> u64 {
    addr & (-(size_of::<u64>() as i64) as u64)
}

pub fn write_byte(pid: Pid, addr: u64, val: u8) -> Result<u8, nix::Error> {
    // Write 8 Byte at a time.
    let aligned_addr = align_addr_to_word(addr);
    let byte_offset = addr - aligned_addr;
    let word = ptrace::read(pid, aligned_addr as ptrace::AddressType)? as u64;
    let orig_byte = (word >> 8 * byte_offset) & 0xff;
    let masked_word = word & !(0xff << 8 * byte_offset);
    let updated_word = masked_word | ((val as u64) << 8 * byte_offset);
    ptrace::write(
        pid,
        aligned_addr as ptrace::AddressType,
        updated_word as *mut std::ffi::c_void,
    )?;
    Ok(orig_byte as u8)
}

pub fn parse_address(addr: &str) -> Option<u64> {
    let addr_without_0x = if addr.to_lowercase().starts_with("0x") {
        &addr[2..]
    } else {
        &addr
    };
    u64::from_str_radix(addr_without_0x, 16).ok()
}
