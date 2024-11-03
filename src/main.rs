extern crate nom;
use itch_parser::MsgStream;
//use std::char;
use std::io::{self};
//use std::fs::File;
use std::time::Instant;
//use nom::{IResult, number::complete::{be_u8, be_u16, be_u32, be_u64}};
//use flate2::read::GzDecoder;
fn main() -> io::Result<()>{
    let mut test_read = MsgStream::from_gz("oct302019ITCH.gz").unwrap();
    let start = Instant::now();
    let _a = test_read.process_bytes();
    let finish = Instant::elapsed(&start);
    println!("{:?}",finish);
    Ok(())
}
