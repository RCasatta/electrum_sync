use bitcoin::util::bip32::ExtendedPubKey;
use bitcoin::{OutPoint, Transaction, TxOut};
use db::*;
use electrum_client::{Client, GetHistoryRes};
use error::*;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Instant;

mod db;
mod error;

fn main() -> Result<(), Error> {
    let xpub = "tpubD6NzVbkrYhZ4YRJLsDvYBaXK6EFi9MSV34h8BAvHPzW8RtUpJpBFiL23hRnvrRUcb6Fz9eKiVG8EzZudGXYfdo5tiP8BuhrsBmFAsREPZG4";
    let xpub = ExtendedPubKey::from_str(xpub)?;
    let db = Forest::new("/tmp/db", xpub)?;

    sync(&db)?;
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

    Ok(())
}

fn sync(db: &Forest) -> Result<(), Error> {
    let start = Instant::now();

    let mut client = Client::new("tn.not.fyi:55001")?;
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
    println!(
        "last_used: {:?} elapsed {}",
        last_used,
        (Instant::now() - start).as_millis()
    );

    let mut txs_to_download = Vec::new();
    for tx_id in history_txs_id.iter() {
        if db.get_tx(tx_id)?.is_none() {
            txs_to_download.push(tx_id);
        }
    }
    if !txs_to_download.is_empty() {
        let txs_downloaded = client.batch_transaction_get(txs_to_download)?;
        println!(
            "txs_downloaded {:?} {}",
            txs_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
        let mut previous_txs_set = HashSet::new();
        for tx in txs_downloaded.iter() {
            db.insert_tx(&tx.txid(), &tx)?;
            for input in tx.input.iter() {
                previous_txs_set.insert(input.previous_output.txid);
            }
        }
        let mut previous_txs_vec = vec![];
        for tx_id in previous_txs_set {
            if db.get_tx(&tx_id)?.is_none() {
                previous_txs_vec.push(tx_id);
            }
        }
        let txs_downloaded = client.batch_transaction_get(&previous_txs_vec)?;
        println!(
            "previous txs_downloaded {:?} {}",
            txs_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
        for tx in txs_downloaded.iter() {
            db.insert_tx(&tx.txid(), tx)?;
        }
    }

    let mut heights_to_download = Vec::new();
    for height in heights_set {
        if db.get_header(height)?.is_none() {
            heights_to_download.push(height);
        }
    }
    if !heights_to_download.is_empty() {
        let headers_downloaded = client.batch_block_header(heights_to_download.clone())?;
        for (header, height) in headers_downloaded.iter().zip(heights_to_download.iter()) {
            db.insert_header(*height, header)?;
        }
        println!(
            "headers_downloaded {:?} {}",
            headers_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
    }

    // sync heights, which are my txs
    for (txid, height) in txid_height.iter() {
        db.insert_height(txid, *height)?; // adding new, but also updating reorged tx
    }
    for (txid_db, _) in db.get_heights()?.iter() {
        if txid_height.get(txid_db).is_none() {
            db.remove_height(txid_db)?; // something in the db is not in live list (rbf), removing
        }
    }

    println!("elapsed {}", (Instant::now() - start).as_millis());

    Ok(())
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
    let heights = db.get_heights()?;
    let mut txs = vec![];

    for (tx_id, height) in heights {
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
    let heights = db.get_heights()?;
    let mut utxos = vec![];
    for (tx_id, _) in heights {
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
