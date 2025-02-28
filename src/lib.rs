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
    pub typ: Option<u8>,
    pub timestamp: Option<u64>,
    pub orrf: Option<u64>,
    pub buy_sell: Option<u8>,
    pub shares: Option<u32>,
    pub price: Option<u32>,
    pub executed_shares: Option<u32>,
    pub executed_price: Option<u32>,
    pub new_orff: Option<u64>,
    pub cancelled_shares: Option<u32>,
    pub bid: Option<u32>,
    pub ask: Option<u32>,
    pub spread: Option<u32>,
    pub ask_depth: Option<u32>,
    pub bid_depth: Option<u32>,
    pub depth: Option<u32>,
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
        let _a = self.read_all_bytes_in();
        loop {
            if self.buffer.len() < 100 {
                /* 
                let bytes_read = self.fetch_bytes()?;
                if bytes_read == 0 || bytes_read < 50{
                    break;
                }*/
                break;
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(88),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    cancelled_shares: Some(cancelled_shares),
                    buy_sell: None,
                    shares: None,
                    price: None,
                    executed_price: None,
                    executed_shares: None,
                    new_orff: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(65),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: Some(buy_sell),
                    shares: Some(shares),
                    price: Some(price),
                    executed_price: None,
                    executed_shares: None,
                    new_orff: None,
                    cancelled_shares: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(80),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: Some(buy_sell),
                    shares: Some(shares),
                    price: Some(price),
                    executed_price: None,
                    executed_shares: None,
                    new_orff: None,
                    cancelled_shares: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(85),
                    timestamp: Some(timestamp),
                    orrf: Some(og_orn),
                    buy_sell: None,
                    shares: Some(shares),
                    price: Some(price),
                    executed_price: None,
                    executed_shares: None,
                    new_orff: Some(new_orn),
                    cancelled_shares: None,
                };
                self.companies
                    .entry(locate)
                    .and_modify(|company| company.push(msg))
                    .or_insert(vec![msg]);
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(69),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: None,
                    shares: None,
                    price: None,
                    executed_price: None,
                    executed_shares: Some(executed_shares),
                    new_orff: None,
                    cancelled_shares: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(67),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: None,
                    shares: None,
                    price: None,
                    executed_price: Some(executed_price),
                    executed_shares: Some(executed_shares),
                    new_orff: None,
                    cancelled_shares: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(68),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: None,
                    shares: None,
                    price: None,
                    executed_price: None,
                    executed_shares: None,
                    new_orff: None,
                    cancelled_shares: None,
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
                    bid_depth: None,
                    ask_depth: None,
                    depth: None,
                    spread: None,
                    bid: None,
                    ask: None,
                    typ: Some(70),
                    timestamp: Some(timestamp),
                    orrf: Some(orn),
                    buy_sell: Some(buy_sell),
                    shares: Some(shares),
                    price: Some(price),
                    executed_price: None,
                    executed_shares: None,
                    new_orff: None,
                    cancelled_shares: None,
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

    pub fn process_order_book(&mut self) -> Result<()> {
        for (_loc, feed) in &mut self.companies {
            //[price, shares]
            let mut bids: HashMap<u64, [u32; 2]> = HashMap::new();
            let mut asks: HashMap<u64, [u32; 2]> = HashMap::new();
            //{price: shares}
            let mut bid_spread: BTreeMap<u32, u32> = BTreeMap::new();
            let mut ask_spread: BTreeMap<u32, u32> = BTreeMap::new();
            for msg in feed {
                if msg.typ == Some(68) {
                    //D order delete
                    let orn = msg.orrf.unwrap();
                    if bids.contains_key(&orn) {
                        let order = bids.remove(&orn).unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= order[1]);
                    } else {
                        let order = asks.remove(&orn).unwrap_or([0, 0]);
                        if order == [0, 0] {
                            continue;
                        }
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= order[1]);
                    }
                } else if msg.typ == Some(65) || msg.typ == Some(70) {
                    //A,F add order
                    if msg.buy_sell == Some(66) {
                        bids.insert(msg.orrf.unwrap(), [msg.price.unwrap(), msg.shares.unwrap()]);
                        bid_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                    } else {
                        asks.insert(msg.orrf.unwrap(), [msg.price.unwrap(), msg.shares.unwrap()]);
                        ask_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                    }
                } else if msg.typ == Some(88) {
                    //X order cancel
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.cancelled_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.cancelled_shares.unwrap());
                    } else {
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.cancelled_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.cancelled_shares.unwrap());
                    }
                } else if msg.typ == Some(67) {
                    //C order executed with price
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                    } else {
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                    }
                } else if msg.typ == Some(69) {
                    //E order executed
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                    } else {
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                    }
                } else if msg.typ == Some(85) {
                    //U order replace
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let del = bids.remove(&msg.orrf.unwrap()).unwrap();
                        bid_spread
                            .entry(del[0])
                            .and_modify(|shares| *shares -= del[1]);
                        bids.insert(
                            msg.new_orff.unwrap(),
                            [msg.price.unwrap(), msg.shares.unwrap()],
                        );
                        bid_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                    } else {
                        let del = asks.remove(&msg.orrf.unwrap()).unwrap();
                        ask_spread
                            .entry(del[0])
                            .and_modify(|shares| *shares -= del[1]);
                        asks.insert(
                            msg.new_orff.unwrap(),
                            [msg.price.unwrap(), msg.shares.unwrap()],
                        );
                        ask_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                    }
                } else if msg.typ == Some(80) {
                    //P trade message do something
                }
                let mut bid: u32 = 0;
                let mut ask: u32 = 0;
                let mut bid_depth: u32 = 0;
                let mut ask_depth: u32 = 0;
                if bid_spread.len() > 0 {
                    bid_depth = bid_spread.values().cloned().sum();
                    let bid_clone = bid_spread.clone();
                    for (k, v) in bid_clone.iter().rev() {
                        if v > &0 {
                            bid = *k;
                            break;
                        }
                    }
                }
                if ask_spread.len() > 0 {
                    ask_depth = ask_spread.values().cloned().sum();
                    let ask_clone = ask_spread.clone();
                    for (k, v) in ask_clone {
                        if v > 0 {
                            ask = k;
                            break;
                        }
                    }
                }
                msg.bid = Some(bid);
                msg.ask = Some(ask);
                msg.spread = Some(ask - bid);
                msg.bid_depth = Some(bid_depth);
                msg.ask_depth = Some(ask_depth);
                msg.depth = Some(bid_depth + ask_depth);
            }
        }
        Ok(())
    }

    pub fn process_order_book2(&mut self) -> Result<()> {
        for (_loc, feed) in &mut self.companies {
            //[price, shares]
            let mut bids: HashMap<u64, [u32; 2]> = HashMap::new();
            let mut asks: HashMap<u64, [u32; 2]> = HashMap::new();
            //{price: shares}
            let mut bid_spread: BTreeMap<u32, u32> = BTreeMap::new();
            let mut ask_spread: BTreeMap<u32, u32> = BTreeMap::new();
            let mut bid: u32 = 0;
            let mut ask: u32 = u32::MAX;
            let mut bid_depth: u32 = 0;
            let mut ask_depth: u32 = 0;
            for msg in feed {
                if msg.typ == Some(68) {
                    //D order delete, done
                    let orn = msg.orrf.unwrap();
                    if bids.contains_key(&orn) {
                        let order = bids.remove(&orn).unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= order[1]);
                        bid_depth -= order[1];
                        //bid
                        if order[0] == bid && bid_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price - 100 * i;
                                if bid_spread.contains_key(&sub) {
                                    if bid_spread.get(&sub).unwrap() > &0 {
                                        bid = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let order = asks.remove(&orn).unwrap_or([0, 0]);
                        if order == [0, 0] {
                            continue;
                        }
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= order[1]);
                        ask_depth -= order[1];
                        //ask
                        if order[0] == ask && ask_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price + 100 * i;
                                if ask_spread.contains_key(&sub) {
                                    if ask_spread.get(&sub).unwrap() > &0 {
                                        ask = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if msg.typ == Some(65) || msg.typ == Some(70) {
                    //A,F add order done
                    if msg.buy_sell == Some(66) {
                        bids.insert(msg.orrf.unwrap(), [msg.price.unwrap(), msg.shares.unwrap()]);
                        bid_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                        if msg.price.unwrap() > bid {
                            bid = msg.price.unwrap();
                        }
                        bid_depth += msg.shares.unwrap();
                    } else {
                        asks.insert(msg.orrf.unwrap(), [msg.price.unwrap(), msg.shares.unwrap()]);
                        ask_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                        if msg.price.unwrap() < ask {
                            ask = msg.price.unwrap();
                        }
                        ask_depth += msg.shares.unwrap();
                    }
                } else if msg.typ == Some(88) {
                    //X order cancel, done
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.cancelled_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.cancelled_shares.unwrap());
                        bid_depth -= msg.cancelled_shares.unwrap();
                        //bid
                        if order[0] == bid && bid_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price - 100 * i;
                                if bid_spread.contains_key(&sub) {
                                    if bid_spread.get(&sub).unwrap() > &0 {
                                        bid = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let mut ref_ord = [0,0];
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap_or(&mut ref_ord);
                        if order == &mut [0,0]{
                            continue;
                        }
                        order[1] -= msg.cancelled_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.cancelled_shares.unwrap());
                        ask_depth -= msg.cancelled_shares.unwrap();
                        //ask
                        if order[0] == ask && ask_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price + 100 * i;
                                if ask_spread.contains_key(&sub) {
                                    if ask_spread.get(&sub).unwrap() > &0 {
                                        ask = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if msg.typ == Some(67) {
                    //C order executed with price, done
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                        bid_depth -= msg.executed_shares.unwrap();
                        //bid
                        if order[0] == bid && bid_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price - 100 * i;
                                if bid_spread.contains_key(&sub) {
                                    if bid_spread.get(&sub).unwrap() > &0 {
                                        bid = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                        ask_depth -= msg.executed_shares.unwrap();
                        //ask
                        if order[0] == ask && ask_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price + 100 * i;
                                if ask_spread.contains_key(&sub) {
                                    if ask_spread.get(&sub).unwrap() > &0 {
                                        ask = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if msg.typ == Some(69) {
                    //E order executed, done
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let order = bids.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        bid_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                        bid_depth -= msg.executed_shares.unwrap();
                        //bid
                        if order[0] == bid && bid_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price - 100 * i;
                                if bid_spread.contains_key(&sub) {
                                    if bid_spread.get(&sub).unwrap() > &0 {
                                        bid = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    } else{
                        let order = asks.get_mut(&msg.orrf.unwrap()).unwrap();
                        order[1] -= msg.executed_shares.unwrap();
                        ask_spread
                            .entry(order[0])
                            .and_modify(|shares| *shares -= msg.executed_shares.unwrap());
                        ask_depth -= msg.executed_shares.unwrap();
                        //ask
                        if order[0] == ask && ask_spread.get(&order[0]).unwrap() == &0 {
                            let ref_price = order[0];
                            for i in 1..1000 {
                                let sub = ref_price + 100 * i;
                                if ask_spread.contains_key(&sub) {
                                    if ask_spread.get(&sub).unwrap() > &0 {
                                        ask = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if msg.typ == Some(85) {
                    //U order replace, done
                    if bids.contains_key(&msg.orrf.unwrap()) {
                        let del = bids.remove(&msg.orrf.unwrap()).unwrap();
                        bid_spread
                            .entry(del[0])
                            .and_modify(|shares| *shares -= del[1]);
                        bids.insert(
                            msg.new_orff.unwrap(),
                            [msg.price.unwrap(), msg.shares.unwrap()],
                        );
                        bid_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                        bid_depth -= del[1];
                        bid_depth += msg.shares.unwrap();
                        //bid
                        if msg.price.unwrap() > bid {
                            bid = msg.price.unwrap();
                        } else if del[0] == bid && bid_spread.get(&del[0]).unwrap() == &0 {
                            let ref_price = del[0];
                            for i in 1..1000 {
                                let sub = ref_price - 100 * i;
                                if bid_spread.contains_key(&sub) {
                                    if bid_spread.get(&sub).unwrap() > &0 {
                                        bid = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let ref_ord = [0,0];
                        let del = asks.remove(&msg.orrf.unwrap()).unwrap_or(ref_ord);
                        if del == [0,0]{
                            continue;
                        }
                        ask_spread
                            .entry(del[0])
                            .and_modify(|shares| *shares -= del[1]);
                        asks.insert(
                            msg.new_orff.unwrap(),
                            [msg.price.unwrap(), msg.shares.unwrap()],
                        );
                        ask_spread
                            .entry(msg.price.unwrap())
                            .and_modify(|shares| *shares += msg.shares.unwrap())
                            .or_insert(msg.shares.unwrap());
                        ask_depth -= del[1];
                        ask_depth += msg.shares.unwrap();
                        //ask
                        if msg.price.unwrap() < ask {
                            ask = msg.price.unwrap();
                        } else if del[0] == ask && ask_spread.get(&del[0]).unwrap() == &0 {
                            let ref_price = del[0];
                            for i in 1..1000 {
                                let sub = ref_price + 100 * i;
                                if ask_spread.contains_key(&sub) {
                                    if ask_spread.get(&sub).unwrap() > &0 {
                                        ask = sub;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if msg.typ == Some(80) {
                    //P trade message do something
                }
                msg.bid = Some(bid);
                msg.ask = Some(ask);
                msg.spread = Some(ask - bid);
                msg.bid_depth = Some(bid_depth);
                msg.ask_depth = Some(ask_depth);
                msg.depth = Some(bid_depth + ask_depth);
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
