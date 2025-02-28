use itch_parser::MsgStream;
use std::env;
use std::io::{self};
//use std::process::Command;
use std::time::Instant;
fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let start = Instant::now();
    let accepted_year = &args[1];

    let mut data_files: Vec<String> = Vec::new();
    let data_folders = std::fs::read_dir("./data")?;
    for folder in data_folders {
        let entry = folder?.file_name().into_string().unwrap();
        data_files.push(entry);
    }

    let mut entries: Vec<String> = Vec::new();
    let dir = std::fs::read_dir("./locates")?;
    for file in dir {
        let entry = file?.file_name().into_string().unwrap();
        let year = entry.get(17..19).unwrap();
        let date = entry.get(19..23).unwrap();
        let full_date = date.to_owned() + year;
        if year == accepted_year && !data_files.contains(&full_date) {
            entries.push(entry);
        }
    }
    entries.sort();
    for entry in entries {
        let year = entry.get(17..19).unwrap();
        let date = entry.get(19..23).unwrap();
        let full_date = date.to_owned() + year;
        println!("{} {}", full_date, accepted_year);

        let dow30 = vec![
            "AAPL", "AXP", "BA", "CAT", "CSCO", "CVX", "DIS", "DWDP", "GE", "GS", "HD", "IBM",
            "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV",
            "UNH", "UTX", "V", "VZ", "WBA", "WMT", "XOM",
        ];
        let tester_read = MsgStream::from_gz_to_buf(format!("itchdata/S{}-v50.txt.gz", &full_date));
        if tester_read.is_err() {
            continue;
        } else {
            let start_file = Instant::now();
            //let mut test_read =
                //MsgStream::from_gz_to_buf(format!("itchdata/S{}-v50.txt.gz", &full_date)).unwrap();
            let mut test_read =
                MsgStream::from_gz_to_reader(format!("itchdata/S{}-v50.txt.gz", &full_date)).unwrap();
            let _a = test_read.get_locate_codes(dow30, &full_date);
            println!("{:?}", &test_read.loc_to_ticker);

            let _b = test_read.process_bytes();
            let process_time = Instant::elapsed(&start_file);
            println!("{:?}", process_time);

            let _c = test_read.process_order_book2();
            let order_time = Instant::elapsed(&start_file) - process_time;
            println!("{:?}", order_time);
            println!("finished date: {}", full_date);
            std::mem::drop(test_read);
        }

        //let _d = test_read.write_companies(&full_date);
    }
    let finish = Instant::elapsed(&start);
    println!("{:?}", finish);
    Ok(())
}
/*
for i in 4..10{
    for j in 4..10{
        let url = format!("https://emi.nasdaq.com/ITCH/Stock_Locate_Codes/bx_stocklocate_20170{}0{}.txt", i,j);
        let output = Command::new("wget").arg("-P").arg("./locates").arg(url).output().expect("didn't get it");
        if output.status.success(){
            println!("downloaded {} {}", i, j);
        }
    }
}*/
