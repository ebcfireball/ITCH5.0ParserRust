use itch_parser::MsgStream;
use std::io::{self};
use std::time::Instant;
use std::{env, thread};
fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let start = Instant::now();


    let handle = thread::spawn(move ||{
        process_from_date(&args[1]).unwrap();
    });
    handle.join().unwrap();


    let finish = Instant::elapsed(&start);
    println!("{:?}", finish);
    Ok(())
}

fn process_from_date(date:&str)->io::Result<()>{
    let mut test_read = MsgStream::from_gz(format!("/data/itchdata/S{}-v50.txt.gz", date)).unwrap();
    let _a = test_read.process_bytes(vec!["AAPL"]);
    let _b = test_read.process_order_book();
    let _c = test_read.write_companies(date);
    Ok(())
}
// /data/itchdata/S{date}-v50.txt.gz
// test on the file on here
// 070918 071018
