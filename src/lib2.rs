use byteorder::{BigEndian, ByteOrder};
use csv::Writer;
use flate2::read::GzDecoder;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{BufReader, Read, Result};
use std::u32;
use std::{fs::File, path::Path};
// monthdayyear

pub const BUFSIZE: usize = 65536;

pub struct MsgStream<R> {
    pub reader: R,
    pub buffer: VecDeque<u8>,
    pub read_calls: u32,
    pub message_ct: usize,
    pub companies: HashMap<u16, Vec<Message>>,
    pub loc_to_ticker: HashMap<u16, String>,
}

#[derive(Serialize, Clone, Copy)]
pub struct Message {
    pub typ: u8,
    pub timestamp: u64,
    pub orrf: u64,
    pub buy_sell: u8,
    pub shares: u32,
    pub price: u32,
    pub bid: u32,
    pub ask: u32,
    pub spread: u32,
    pub ask_depth: u32,
    pub bid_depth: u32,
    pub depth: u32,
}

impl<R: Read> MsgStream<R> {
    pub fn from_reader(reader: R) -> MsgStream<R> {
        MsgStream::new(reader)
    }

    pub fn new(reader: R) -> MsgStream<R> {
        MsgStream {
            reader,
            buffer: VecDeque::new(),
            companies: HashMap::new(),
            read_calls: 0,
            message_ct: 0,
            loc_to_ticker: HashMap::new(),
        }
    }

    pub fn get_locate_codes(&mut self, focused_names: Vec<&str>, date: &str) -> Result<()> {
        let reformated = format!("{}{}", date.split_at(4).1, date.split_at(4).0);
        let file =
            std::fs::read_to_string(format!("locates/bx_stocklocate_20{}.txt", { reformated }))
                .unwrap();
        let splited: Vec<&str> = file.split_terminator('\n').collect();
        for stock in splited {
            let temp_split: Vec<&str> = stock.split_terminator(',').collect();
            if focused_names.contains(&temp_split[0]) {
                self.loc_to_ticker
                    .insert(temp_split[1].parse().unwrap(), temp_split[0].to_string());
            }
        }
        Ok(())
    }

    pub fn process_bytes(&mut self) -> Result<()> {
        //let _a = self.read_all_bytes_in();
        loop {
            if self.buffer.len() < 100 {
                let bytes_read = self.fetch_bytes()?;
                if bytes_read == 0 {
                    break;
                }
            }

            let curr = self.buffer.pop_front();
            self.message_ct += 1;
            //println!("{},{}", self.loc_to_ticker.len(), self.companies.len());

            if curr == Some(88u8) {
                //X
                let mut data = self.buffer.drain(..24);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let cancelled_shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 88,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: 0,
                    shares: cancelled_shares,
                    price: 0,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(65u8) {
                //A
                let mut data = self.buffer.drain(..37);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                //
                //let tracking_num = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                //
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                //try this
                //let arr: [u8; 6] = std::array::from_fn(|_| data.next().unwrap());
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let buy_sell = data.next().unwrap();
                let shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                data.nth(7);
                let price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                //print!("A ,{locate} ,{tracking_num} ,{timestamp} ,{orn} ,{buy_sell} ,{shares} ,AAPL ,{price} ");
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 65,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: buy_sell,
                    shares: shares,
                    price: price,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(80u8) {
                //P
                let mut data = self.buffer.drain(..45);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let buy_sell = data.next().unwrap();
                let shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                data.nth(7);
                let price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 80,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: buy_sell,
                    shares: shares,
                    price: price,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(85u8) {
                //U
                let mut data = self.buffer.drain(..36);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let og_orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let new_orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 85,
                    timestamp: timestamp,
                    orrf: og_orn,
                    buy_sell: 0,
                    shares: shares,
                    price: price,
                };
                let msg2 = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 65,
                    timestamp: timestamp,
                    orrf: new_orn,
                    buy_sell: 0,
                    shares: shares,
                    price: price,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg2))
                    .or_insert(vec![msg2]);
            } else if curr == Some(69u8) {
                //E
                let mut data = self.buffer.drain(..32);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                //let tracking_num = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let executed_shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                //let match_num = BigEndian::read_u64(&[
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //]);

                //print!("E ,{locate} ,{tracking_num} ,{timestamp} ,{orn} ,{executed_shares} ,{match_num} ");
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 69,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: 0,
                    shares: executed_shares,
                    price: 0,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(67u8) {
                //C
                let mut data = self.buffer.drain(..37);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                //let tracking_num = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let executed_shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                //let match_num = BigEndian::read_u64(&[
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //data.next().unwrap(),
                //]);
                //let printable = data.next().unwrap() as char;
                data.nth(8);
                let executed_price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                //print!("C ,{locate} ,{tracking_num} ,{timestamp} ,{orn} ,{executed_shares} ,{match_num} ,{printable} ,{executed_price} ");
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 67,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: 0,
                    shares: executed_shares,
                    price: executed_price,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(68u8) {
                //D
                let mut data = self.buffer.drain(..20);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 68,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: 0,
                    shares: 0,
                    price: 0,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(70u8) {
                //F
                let mut data = self.buffer.drain(..41);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
                if !self.loc_to_ticker.contains_key(&locate) {
                    continue;
                }
                data.nth(1);
                let timestamp = BigEndian::read_u48(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let orn = BigEndian::read_u64(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let buy_sell = data.next().unwrap();
                let shares = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                data.nth(7);
                let price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                let msg = Message {
                    bid_depth: 0,
                    ask_depth: 0,
                    depth: 0,
                    spread: 0,
                    bid: 0,
                    ask: 0,
                    typ: 70,
                    timestamp: timestamp,
                    orrf: orn,
                    buy_sell: buy_sell,
                    shares: shares,
                    price: price,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
            } else if curr == Some(83u8) {
                //S
                self.buffer.drain(..13);
            } else if curr == Some(82u8) {
                //R
                self.buffer.drain(..40);
            } else if curr == Some(72u8) {
                //H
                self.buffer.drain(..26);
            } else if curr == Some(89u8) {
                //Y
                self.buffer.drain(..21);
            } else if curr == Some(76u8) {
                //L
                self.buffer.drain(..27);
            } else if curr == Some(86u8) {
                //V
                self.buffer.drain(..36);
            } else if curr == Some(87u8) {
                //W
                self.buffer.drain(..13);
            } else if curr == Some(74u8) {
                //J
                self.buffer.drain(..36);
            } else if curr == Some(104u8) {
                //h
                self.buffer.drain(..22);
            } else if curr == Some(81u8) {
                //Q
                self.buffer.drain(..41);
            } else if curr == Some(66u8) {
                //B
                self.buffer.drain(..20);
            } else if curr == Some(73u8) {
                //I
                self.buffer.drain(..51);
            } else if curr == Some(78u8) {
                //N
                self.buffer.drain(..21);
            } else {
            }
        }
        print!(
            "{},{},{},",
            self.message_ct,
            self.companies.len(),
            self.read_calls
        );
        Ok(())
    }
    pub fn read_all_bytes_in(&mut self) -> Result<()> {
        let mut temp_buffer = Vec::new();
        self.reader.read_to_end(&mut temp_buffer).unwrap();
        self.buffer.extend(temp_buffer);
        Ok(())
    }

    pub fn fetch_bytes(&mut self) -> Result<usize> {
        self.read_calls += 1;
        let mut temp_buffer = [0; BUFSIZE];
        let bytes_read = self.reader.read(&mut temp_buffer[..])?;
        self.buffer.extend(&temp_buffer[..bytes_read]);
        Ok(bytes_read)
    }

    pub fn process_order_book_py(&mut self) -> Result<()> {
        for (_loc, feed) in &mut self.companies {
            //[price, shares]
            let mut bids: HashMap<u64, [u32; 2]> = HashMap::new();
            let mut asks: HashMap<u64, [u32; 2]> = HashMap::new();
            //{price: shares}
            let mut bid_spread: BTreeMap<u32, u32> = BTreeMap::new();
            let mut ask_spread: BTreeMap<u32, u32> = BTreeMap::new();
            let mut bid_depth: u32 = 0;
            let mut ask_depth: u32 = 0;
            let mut u_prev = 0;
            for msg in feed {
                //a,f,d,x,c,e,u
                //a,f
                let typ = msg.typ;
                if typ == 65 || typ == 70 {
                    let shares = msg.shares;
                    let price = msg.price;
                    if msg.buy_sell == 66 || (msg.buy_sell == 0 && u_prev == 66) {
                        bids.insert(msg.orrf, [price, shares]);
                        bid_depth += shares;
                        bid_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares += shares)
                            .or_insert(shares);
                    } else {
                        asks.insert(msg.orrf, [price, shares]);
                        ask_depth += shares;
                        ask_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares += shares)
                            .or_insert(shares);
                    }
                } else if typ == 68 || typ == 85 {
                    //d, u
                    if bids.contains_key(&msg.orrf) {
                        let ord = bids.remove(&msg.orrf).unwrap();
                        let shares = ord[1];
                        let price = ord[0];
                        bid_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares -= shares);
                        if bid_spread.get(&price).unwrap() == &0 {
                            bid_spread.remove(&price);
                        }
                        if typ == 85 {
                            u_prev = 66;
                        }
                        bid_depth -= shares;
                    } else if asks.contains_key(&msg.orrf) {
                        let ord = asks.remove(&msg.orrf).unwrap();
                        let shares = ord[1];
                        let price = ord[0];
                        ask_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares -= shares);
                        if ask_spread.get(&price).unwrap() == &0 {
                            ask_spread.remove(&price);
                        }
                        if typ == 85 {
                            u_prev = 83;
                        }
                        ask_depth -= shares;
                    }
                } else if typ == 67 || typ == 69 || typ == 88 {
                    //c,e,x
                    if bids.contains_key(&msg.orrf) {
                        let order = bids.get(&msg.orrf).unwrap();
                        let shares = msg.shares;
                        let shares_rem = order[1] - shares;
                        let price = order[0];
                        if shares_rem == 0 {
                            bids.remove(&msg.orrf);
                        } else {
                            bids.entry(msg.orrf).and_modify(|ord| ord[1] = shares_rem);
                        }
                        bid_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares -= shares);
                        if bid_spread.get(&price).unwrap() == &0 {
                            bid_spread.remove(&price);
                        }
                        bid_depth -= shares;
                    } else if asks.contains_key(&msg.orrf) {
                        let order = asks.get(&msg.orrf).unwrap();
                        let shares = msg.shares;
                        let shares_rem = order[1] - shares;
                        let price = order[0];
                        if shares_rem == 0 {
                            asks.remove(&msg.orrf);
                        } else {
                            asks.entry(msg.orrf).and_modify(|ord| ord[1] = shares_rem);
                        }
                        ask_spread
                            .entry(price)
                            .and_modify(|curr_shares| *curr_shares -= shares);
                        if ask_spread.get(&price).unwrap() == &0 {
                            ask_spread.remove(&price);
                        }
                        ask_depth -= shares;
                    }
                }

                let bid = if let Some((bpr, _bshr)) = bid_spread.last_key_value() {
                    *bpr
                } else {
                    0
                };
                let ask = if let Some((apr, _ashr)) = ask_spread.first_key_value() {
                    *apr
                } else {
                    0
                };
                msg.bid = bid;
                msg.ask = ask;
                msg.spread = if ask >= bid { ask - bid } else { 0 };
                msg.ask_depth = ask_depth;
                msg.bid_depth = bid_depth;
                msg.depth = ask_depth + bid_depth;
            }
        }
        Ok(())
    }

    pub fn write_companies(&mut self, date: &str) -> Result<()> {
        std::fs::create_dir(format!("data/{date}"))?;
        for (loc, feed) in &self.companies {
            println!("writing to {}", loc);
            let name = self.loc_to_ticker.get(loc).unwrap();
            let mut wtr = Writer::from_path(format!("data/{date}/{name}.csv"))?;
            //let mut wtr = Writer::from_path(format!("data/{name}.csv"))?;
            for msg in feed {
                wtr.serialize(msg)?;
            }
        }
        Ok(())
    }
}
impl MsgStream<BufReader<GzDecoder<File>>> {
    pub fn from_gz_to_buf<P: AsRef<Path>>(
        path: P,
    ) -> Result<MsgStream<BufReader<GzDecoder<File>>>> {
        let file = File::open(path)?;
        let readerr = GzDecoder::new(file);
        let reader = BufReader::new(readerr);
        Ok(MsgStream::from_reader(reader))
    }
}

impl MsgStream<GzDecoder<File>> {
    pub fn from_gz_to_reader<P: AsRef<Path>>(path: P) -> Result<MsgStream<GzDecoder<File>>> {
        let file = File::open(path)?;
        let reader = GzDecoder::new(file);
        Ok(MsgStream::from_reader(reader))
    }
}
