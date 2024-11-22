use byteorder::{BigEndian, ByteOrder};
use csv::Writer;
use flate2::read::GzDecoder;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{BufReader, Read, Result};
use std::{fs::File, path::Path};

pub const BUFSIZE: usize = 65536;

pub struct MsgStream<R> {
    pub reader: R,
    pub buffer: VecDeque<u8>,
    pub read_calls: u32,
    pub message_ct: u32,
    pub companies: HashMap<u16, Vec<A>>,
    pub error: bool,
}

#[derive(Serialize)]
pub struct A {
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

pub struct Order {
    pub price: u32,
    pub re: u64,
    pub shares: u32,
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
            error: false,
        }
    }
    pub fn process_bytes(&mut self) -> Result<()> {
        let mut temp_buffer = vec![0; BUFSIZE];
        let mut bytes_read = self.reader.read(&mut temp_buffer)?;
        self.buffer.extend(&temp_buffer[..bytes_read]);
        self.read_calls += 1;

        loop {
            let curr = self.buffer.pop_front();
            if self.buffer.len() < 60 {
                if self.read_calls > 5500 {
                    break;
                }
                bytes_read = self.reader.read(&mut temp_buffer)?;
                if bytes_read == 0 {
                    break;
                }
                self.buffer.extend(&temp_buffer[..bytes_read]);
                self.read_calls += 1;
            }

            self.message_ct += 1;

            if curr == Some(88u8) {
                //X
                let mut data = self.buffer.drain(..24);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(65u8) {
                //A
                let mut data = self.buffer.drain(..37);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(80u8) {
                //P
                let mut data = self.buffer.drain(..45);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(85u8) {
                //U
                let mut data = self.buffer.drain(..36);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(69u8) {
                //E
                let mut data = self.buffer.drain(..32);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(67u8) {
                //C
                let mut data = self.buffer.drain(..37);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                data.nth(8);
                let executed_price = BigEndian::read_u32(&[
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                    data.next().unwrap(),
                ]);
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(68u8) {
                //D
                let mut data = self.buffer.drain(..20);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
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
                    }]);
            } else if curr == Some(70u8) {
                //F
                let mut data = self.buffer.drain(..41);
                let locate = BigEndian::read_u16(&[data.next().unwrap(), data.next().unwrap()]);
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
                self.companies
                    .entry(locate)
                    .and_modify(|company| {
                        company.push(A {
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
                        })
                    })
                    .or_insert(vec![A {
                        bid_depth: None,
                        ask_depth: None,
                        depth: None,
                        spread: None,
                        bid: None,
                        ask: None,
                        typ: Some(70),
                        timestamp: Some(timestamp),
                        orrf: Some(orn),
                        buy_sell: None,
                        shares: Some(shares),
                        price: Some(price),
                        executed_price: None,
                        executed_shares: None,
                        new_orff: None,
                        cancelled_shares: None,
                    }]);
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

    pub fn fetch_bytes(&mut self) -> Result<usize> {
        self.read_calls += 1;
        let mut temp_buffer = vec![0; BUFSIZE];
        Ok(self.reader.read(&mut temp_buffer)?)
    }

    pub fn process_order_book(&mut self) -> Result<()> {
        for (_loc, feed) in &mut self.companies {
            let mut bids: Vec<Order> = vec![];
            let mut asks: Vec<Order> = vec![];
            let mut ask_depth = 0;
            let mut bid_depth = 0;
            for order in feed {
                if order.typ == Some(68) {
                    //D order delete
                    if let Some(index) = bids.iter().position(|re| Some(re.re) == order.orrf) {
                        bids.remove(index);
                    } else {
                        let position = asks.iter().position(|re| Some(re.re) == order.orrf);
                        asks.remove(position.unwrap());
                    }
                } else if order.typ == Some(65) {
                    //A add order
                    if order.buy_sell == Some(66) {
                        bids.push(Order {
                            price: order.price.unwrap(),
                            re: order.orrf.unwrap(),
                            shares: order.shares.unwrap(),
                        });
                        bid_depth += order.shares.unwrap();
                    } else {
                        asks.push(Order {
                            price: order.price.unwrap(),
                            re: order.orrf.unwrap(),
                            shares: order.shares.unwrap(),
                        });
                        ask_depth += order.shares.unwrap()
                    }
                } else if order.typ == Some(70) {
                    //F add order
                    if order.buy_sell == Some(66) {
                        bids.push(Order {
                            price: order.price.unwrap(),
                            re: order.orrf.unwrap(),
                            shares: order.shares.unwrap(),
                        });
                        bid_depth += order.shares.unwrap();
                    } else {
                        asks.push(Order {
                            price: order.price.unwrap(),
                            re: order.orrf.unwrap(),
                            shares: order.shares.unwrap(),
                        });
                        ask_depth += order.shares.unwrap();
                    }
                } else if order.typ == Some(88) {
                    //X order cancel
                    if let Some(index) = bids.iter().position(|re| Some(re.re) == order.orrf) {
                        if let Some(ord) = bids.get_mut(index) {
                            ord.shares -= &order.cancelled_shares.unwrap();
                            bid_depth -= &order.cancelled_shares.unwrap();
                        }
                    } else {
                        let position = asks.iter().position(|re| Some(re.re) == order.orrf);
                        if let Some(ord) = asks.get_mut(position.unwrap()) {
                            ord.shares -= &order.cancelled_shares.unwrap();
                            ask_depth -= &order.cancelled_shares.unwrap();
                        }
                    }
                } else if order.typ == Some(67) {
                    //C
                    if let Some(index) = bids.iter().position(|re| Some(re.re) == order.orrf) {
                        bid_depth -= &order.executed_shares.unwrap();
                        if bids[index].shares - order.executed_shares.unwrap() == 0 {
                            bids.remove(index);
                        } else {
                            bids[index].shares =
                                bids[index].shares - order.executed_shares.unwrap();
                        }
                    }
                    if let Some(index) = asks.iter().position(|re| Some(re.re) == order.orrf) {
                        ask_depth -= &order.executed_shares.unwrap();
                        if asks[index].shares - order.executed_shares.unwrap() == 0 {
                            asks.remove(index);
                        } else {
                            asks[index].shares =
                                asks[index].shares - order.executed_shares.unwrap();
                        }
                    }
                } else if order.typ == Some(69) {
                    //E
                    if let Some(index) = bids.iter().position(|re| Some(re.re) == order.orrf) {
                        bid_depth -= &order.executed_shares.unwrap();
                        if bids[index].shares - order.executed_shares.unwrap() == 0 {
                            bids.remove(index);
                        } else {
                            bids[index].shares =
                                bids[index].shares - order.executed_shares.unwrap();
                        }
                    }
                    if let Some(index) = asks.iter().position(|re| Some(re.re) == order.orrf) {
                        ask_depth -= &order.executed_shares.unwrap();
                        if asks[index].shares - order.executed_shares.unwrap() == 0 {
                            asks.remove(index);
                        } else {
                            asks[index].shares =
                                asks[index].shares - order.executed_shares.unwrap();
                        }
                    }
                } else if order.typ == Some(85) {
                    //U order replace
                    if let Some(index) = bids.iter().position(|re| Some(re.re) == order.orrf) {
                        if let Some(ord) = bids.get_mut(index) {
                            bid_depth += &ord.shares - &order.shares.unwrap();
                            ord.price = order.price.unwrap();
                            ord.shares = order.shares.unwrap();
                            ord.re = order.new_orff.unwrap();
                        }
                    } else {
                        let position = asks.iter().position(|re| Some(re.re) == order.orrf);
                        if let Some(ord) = asks.get_mut(position.unwrap()) {
                            ask_depth += &ord.shares - &order.shares.unwrap();
                            ord.price = order.price.unwrap();
                            ord.shares = order.shares.unwrap();
                            ord.re = order.new_orff.unwrap();
                        }
                    }
                } else if order.typ == Some(80) {
                    //P trade message do something
                }
                bids.sort_by_key(|ord| ord.price);
                bids.reverse();
                asks.sort_by_key(|ord| ord.price);
                if bids.len() > 0 && asks.len() > 0 {
                    order.ask = Some(asks[0].price);
                    order.bid = Some(bids[0].price);
                    order.spread = Some(asks[0].price - bids[0].price);
                } else if bids.len() > 0 && asks.len() == 0 {
                    order.ask = Some(0);
                    order.bid = Some(bids[0].price);
                    order.spread = Some(bids[0].price);
                } else if bids.len() == 0 && asks.len() > 0 {
                    order.ask = Some(asks[0].price);
                    order.bid = Some(0);
                    order.spread = Some(asks[0].price);
                } else {
                    order.bid = Some(0);
                    order.ask = Some(0);
                    order.spread = Some(0);
                }
                order.ask_depth = Some(ask_depth);
                order.bid_depth = Some(bid_depth);
                order.depth = Some(ask_depth + bid_depth);
            }
        }
        Ok(())
    }

    pub fn write_companies(&mut self) -> Result<()> {
        for (loc, feed) in &self.companies {
            let mut wtr = Writer::from_path(format!("data/{}oct302019.csv", loc))?;
            for msg in feed {
                wtr.serialize(msg)?;
            }
        }
        Ok(())
    }
}

impl MsgStream<BufReader<GzDecoder<File>>> {
    pub fn from_gz<P: AsRef<Path>>(path: P) -> Result<MsgStream<BufReader<GzDecoder<File>>>> {
        let file = File::open(path)?;
        let readerr = GzDecoder::new(file);
        let reader = BufReader::new(readerr);
        Ok(MsgStream::from_reader(reader))
    }
}
