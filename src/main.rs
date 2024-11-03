extern crate nom;
use itch_parser::MsgStream;
use std::io::{self};
use std::time::Instant;
fn main() -> io::Result<()>{
    let mut test_read = MsgStream::from_gz("oct302019ITCH.gz").unwrap();
    let start = Instant::now();
    let _a = test_read.process_bytes();
    let finish = Instant::elapsed(&start);
    println!("{:?}",finish);
    Ok(())
}
