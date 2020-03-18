use bitcoin::util::bip32::ExtendedPubKey;
use bitcoin::{OutPoint, Script, Transaction, TxOut, Txid};
use db::*;
use electrum_client::client::{ElectrumSslStream, ToSocketAddrsDomain};
use electrum_client::{Client, GetHistoryRes};
use error::*;
use log::{info, Level, LevelFilter, Metadata, Record};
use std::collections::{HashMap, HashSet};
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

mod db;
mod error;

pub struct RetryClient<'a, S>
where
    S: Read + Write,
{
    socket_addr: &'a str,
    validate_domain: bool,
    client: Client<S>,
    attempts: u8,
}

impl<'a> RetryClient<'a, ElectrumSslStream> {
    pub fn new_ssl(socket_addr: &'a str, validate_domain: bool) -> Result<Self, Error> {
        let client = Client::new_ssl(socket_addr, validate_domain)?;
        let attempts = 0u8;
        Ok(RetryClient {
            client,
            attempts,
            socket_addr,
            validate_domain,
        })
    }
    pub fn batch_script_get_history<'s, I>(
        &mut self,
        scripts: I,
    ) -> Result<Vec<Vec<GetHistoryRes>>, Error>
    where
        I: IntoIterator<Item = &'s Script> + Clone,
    {
        loop {
            match self.client.batch_script_get_history(scripts.clone()) {
                Ok(result) => {
                    self.attempts = 0;
                    return Ok(result);
                }
                Err(e) => {
                    println!("Error, attempts:{}", self.attempts);
                    if self.attempts > 3 {
                        println!("attempts > 3, giving up");
                        return err("giving up");
                    } else {
                        println!("Creating new client");
                        self.attempts += 1;
                        self.client = Client::new_ssl(self.socket_addr, self.validate_domain)?;
                    }
                }
            }
        }
    }
}

fn main() -> Result<(), Error> {
    init_logger(3);
    let xpub = "tpubD6NzVbkrYhZ4YRJLsDvYBaXK6EFi9MSV34h8BAvHPzW8RtUpJpBFiL23hRnvrRUcb6Fz9eKiVG8EzZudGXYfdo5tiP8BuhrsBmFAsREPZG4";
    //let xpub = "tpubD6NzVbkrYhZ4Xz3UW47QhZBejbwrU4khTztuBoN8tpANN7Mu4St3cWgSUkrZc8v9FbFZaLwCDPHo8gKW3R1GqNTADCSrHpGkAVMyEKUbz4q";
    let xpub = ExtendedPubKey::from_str(xpub)?;
    let db = Forest::new("/tmp/db", xpub)?;

    let elapsed = sync(&db)?;
    let txs = list_tx(&db)?;
    for tx_meta in txs {
        println!(
            "{} {:?} {:?} fee:{:6} my_in:{:8} my_out:{:8}",
            tx_meta.tx.txid(),
            tx_meta.height,
            tx_meta.time,
            tx_meta.fee,
            tx_meta.my_in,
            tx_meta.my_out,
        );
    }
    let utxos = utxo(&db)?;
    let mut balance = 0u64;
    for utxo in utxos {
        println!("{} {}", utxo.0, utxo.1.value);
        balance += utxo.1.value;
    }
    println!("balance {}", balance);

    println!("sync elapsed {}", elapsed);

    Ok(())
}

fn sync(db: &Forest) -> Result<u128, Error> {
    let start = Instant::now();

    //let mut client = Client::new_proxy("ozahtqwp25chjdjd.onion:50001", "127.0.0.1:9050").unwrap();
    //let mut client = Client::new_proxy("explorerzydxu5ecjrkwceayqybizmpjjznk5izmitf2modhcusuqlid.onion:143", "127.0.0.1:9050")?;
    let mut client = RetryClient::new_ssl("blockstream.info:993", true)?;
    thread::sleep(Duration::from_secs(300)); // Error: Error("IOError(Kind(UnexpectedEof))")

    let mut history_txs_id = HashSet::new();
    let mut heights_set = HashSet::new();
    let mut txid_height = HashMap::new();

    let mut last_used = [0u32; 2];
    for i in 0..=1 {
        let mut batch_count = 0;
        loop {
            let batch = db.get_script_batch(i, batch_count)?;
            let result: Vec<Vec<GetHistoryRes>> = client.batch_script_get_history(&batch)?;
            let max = result
                .iter()
                .enumerate()
                .filter(|(_, v)| !v.is_empty())
                .map(|(i, _)| i as u32)
                .max();
            if let Some(max) = max {
                last_used[i as usize] = max
            };

            let flattened: Vec<GetHistoryRes> = result.into_iter().flatten().collect();
            println!("{}/batch({}) {:?}", i, batch_count, flattened.len());

            if flattened.is_empty() {
                break;
            }

            for el in flattened {
                if el.height >= 0 {
                    heights_set.insert(el.height as u32);
                    txid_height.insert(el.tx_hash, el.height as u32);
                }
                history_txs_id.insert(el.tx_hash);
            }

            batch_count += 1;
        }
    }
    db.insert_index(0, last_used[0])?;
    db.insert_index(1, last_used[1])?;
    println!("last_used: {:?}", last_used,);
    /*
    let mut txs_in_db = db.get_all_txid()?;
    let txs_to_download: Vec<&Txid> = history_txs_id.difference(&txs_in_db).collect();
    if !txs_to_download.is_empty() {
        let txs_downloaded = client.batch_transaction_get(txs_to_download)?;
        println!("txs_downloaded {:?}", txs_downloaded.len());
        let mut previous_txs_to_download = HashSet::new();
        for tx in txs_downloaded.iter() {
            db.insert_tx(&tx.txid(), &tx)?;
            txs_in_db.insert(tx.txid());
            for input in tx.input.iter() {
                previous_txs_to_download.insert(input.previous_output.txid);
            }
        }
        let txs_to_download: Vec<&Txid> = previous_txs_to_download.difference(&txs_in_db).collect();
        if !txs_to_download.is_empty() {
            let txs_downloaded = client.batch_transaction_get(txs_to_download)?;
            println!("previous txs_downloaded {:?}", txs_downloaded.len());
            for tx in txs_downloaded.iter() {
                db.insert_tx(&tx.txid(), tx)?;
            }
        }
    }

    let heights_in_db = db.get_only_heights()?;
    let heights_to_download: Vec<u32> = heights_set.difference(&heights_in_db).cloned().collect();
    if !heights_to_download.is_empty() {
        let headers_downloaded = client.batch_block_header(heights_to_download.clone())?;
        for (header, height) in headers_downloaded.iter().zip(heights_to_download.iter()) {
            db.insert_header(*height, header)?;
        }
        println!("headers_downloaded {:?}", headers_downloaded.len());
    }

    // sync heights, which are my txs
    for (txid, height) in txid_height.iter() {
        db.insert_height(txid, *height)?; // adding new, but also updating reorged tx
    }
    for txid_db in db.get_only_txids()?.iter() {
        if txid_height.get(txid_db).is_none() {
            db.remove_height(txid_db)?; // something in the db is not in live list (rbf), removing
        }
    }
    */

    Ok((Instant::now() - start).as_millis())
}

struct TransactionMeta {
    tx: Transaction,
    fee: u64,
    my_in: u64,
    my_out: u64,
    height: Option<u32>,
    time: Option<u32>,
}

fn list_tx(db: &Forest) -> Result<Vec<TransactionMeta>, Error> {
    let (_, all_txs) = db.get_all_spent_and_txs()?;
    let mut txs = vec![];

    for (tx_id, height) in db.get_my()? {
        let tx = all_txs.get(&tx_id).ok_or_else(fn_err("no tx"))?;
        let header = height
            .map(|h| db.get_header(h)?.ok_or_else(fn_err("no header")))
            .transpose()?;
        let total_output: u64 = tx.output.iter().map(|o| o.value).sum();
        let total_input: u64 = tx
            .input
            .iter()
            .filter_map(|i| all_txs.get_previous_value(&i.previous_output))
            .sum();
        let fee = total_input - total_output;
        let my_in: u64 = tx
            .output
            .iter()
            .filter(|o| db.is_mine(&o.script_pubkey))
            .map(|o| o.value)
            .sum();
        let my_out: u64 = tx
            .input
            .iter()
            .filter_map(|i| all_txs.get_previous_output(&i.previous_output))
            .filter(|o| db.is_mine(&o.script_pubkey))
            .map(|o| o.value)
            .sum();
        let tx_meta = TransactionMeta {
            tx: tx.clone(),
            height,
            time: header.map(|h| h.time),
            fee,
            my_in,
            my_out,
        };
        txs.push(tx_meta);
    }
    txs.sort_by(|a, b| {
        b.time
            .unwrap_or(std::u32::MAX)
            .cmp(&a.time.unwrap_or(std::u32::MAX))
    });
    Ok(txs)
}

fn utxo(db: &Forest) -> Result<Vec<(OutPoint, TxOut)>, Error> {
    let (spent, all_txs) = db.get_all_spent_and_txs()?;
    let mut utxos = vec![];
    for tx_id in db.get_only_txids()? {
        let tx = all_txs.get(&tx_id).ok_or_else(fn_err("no tx"))?;
        let tx_utxos: Vec<(OutPoint, TxOut)> = tx
            .output
            .clone()
            .into_iter()
            .enumerate()
            .map(|(vout, output)| (OutPoint::new(tx.txid(), vout as u32), output))
            .filter(|(_, output)| db.is_mine(&output.script_pubkey))
            .filter(|(outpoint, _)| !spent.contains(&outpoint))
            .collect();
        utxos.extend(tx_utxos);
    }
    utxos.sort_by(|a, b| b.1.value.cmp(&a.1.value));
    Ok(utxos)
}

static LOGGER: SimpleLogger = SimpleLogger;

pub struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if record.level() <= Level::Warn {
                println!("{} - {}", record.level(), record.args());
            } else {
                println!("{}", record.args());
            }
        }
    }

    fn flush(&self) {}
}

pub fn init_logger(verbose: u8) {
    //TODO write log message to file
    let level = match verbose {
        0 => LevelFilter::Off,
        1 => LevelFilter::Info,
        _ => LevelFilter::Debug,
    };
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(level))
        .expect("cannot initialize logging");
}
