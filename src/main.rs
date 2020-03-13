use bitcoin::consensus::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{All, Secp256k1};
use bitcoin::util::bip32::{ChildNumber, ExtendedPubKey};
use bitcoin::{Address, BlockHeader, Network, OutPoint, Script, Transaction, TxOut, Txid};
use electrum_client::{Client, GetHistoryRes};
use sled;
use sled::Tree;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::ops::Deref;
use std::ops::DerefMut;
use std::str::FromStr;
use std::time::Instant;

const BATCH_SIZE: u32 = 20;

/// DB
/// Txid, Transaction      contains all my tx and all prevouts
/// Txid, Height           contains only my tx heights
/// Height, BlockHeader    contains all headers at the height of my txs
/// Script, Path           contains all my script up to an empty batch of BATCHSIZE
/// Path, Script           inverse of the previous

struct Forest {
    txs: Tree,
    heights: Tree,
    headers: Tree,
    scripts: Tree,
    paths: Tree,
    secp: Secp256k1<All>,
    xpub: ExtendedPubKey,
}
impl Forest {
    fn new(path: &str, xpub: ExtendedPubKey) -> Self {
        let db = sled::open(path).unwrap();
        Forest {
            txs: db.open_tree("txs").unwrap(),
            heights: db.open_tree("heights").unwrap(),
            headers: db.open_tree("headers").unwrap(),
            scripts: db.open_tree("scripts").unwrap(),
            paths: db.open_tree("paths").unwrap(),
            secp: Secp256k1::new(),
            xpub,
        }
    }

    pub fn get_heights(&self) -> Vec<(Txid, Option<u32>)> {
        let mut heights = vec![];
        for keyvalue in self.heights.iter() {
            let (key, value) = keyvalue.unwrap();
            let txid = Txid::from_slice(&key).unwrap();
            let height = Height::from_slice(&value).1;
            let height = if height == 0 { None } else { Some(height) };
            heights.push((txid, height));
        }
        heights
    }

    pub fn get_all_spent_and_txs(&self) -> (HashSet<OutPoint>, Transactions) {
        let mut txs = Transactions::default();
        let mut spent = HashSet::new();
        for keyvalue in self.txs.iter() {
            let (key, value) = keyvalue.unwrap();
            let txid = Txid::from_slice(&key).unwrap();
            let tx: Transaction = deserialize(&value).unwrap();
            for input in tx.input.iter() {
                spent.insert(input.previous_output);
            }
            txs.insert(txid, tx);
        }
        (spent, txs)
    }

    pub fn get_tx(&self, txid: &Txid) -> Option<Transaction> {
        self.txs
            .get(txid)
            .unwrap()
            .map(|v| deserialize(&v).unwrap())
    }

    pub fn insert_tx(&self, txid: &Txid, tx: &Transaction) {
        self.txs.insert(txid, serialize(tx)).unwrap();
    }

    pub fn get_header(&self, height: u32) -> Option<BlockHeader> {
        self.headers
            .get(Height::new(height))
            .unwrap()
            .map(|v| deserialize(&v).unwrap())
    }

    pub fn insert_header(&self, height: u32, header: &BlockHeader) {
        self.headers
            .insert(Height::new(height), serialize(header))
            .unwrap();
    }

    pub fn remove_height(&self, txid: &Txid) {
        self.heights.remove(txid).unwrap();
    }

    pub fn insert_height(&self, txid: &Txid, height: u32) {
        self.heights
            .insert(txid, Height::new(height).as_ref())
            .unwrap();
    }

    pub fn get_script(&self, path: &Path) -> Option<Script> {
        self.scripts
            .get(path)
            .unwrap()
            .map(|v| deserialize(&v).unwrap())
    }

    pub fn insert_script(&self, path: &Path, script: &Script) {
        self.scripts.insert(path, serialize(script)).unwrap();
    }

    pub fn get_path(&self, script: &Script) -> Option<Path> {
        self.paths
            .get(script.as_bytes())
            .unwrap()
            .map(|v| Path::from_slice(&v))
    }

    pub fn insert_path(&self, script: &Script, path: &Path) {
        self.paths.insert(script.as_bytes(), path.as_ref()).unwrap();
    }

    pub fn is_mine(&self, script: &Script) -> bool {
        self.get_path(script).is_some()
    }

    pub fn get_script_batch(&self, int_or_ext: u32, batch: u32) -> Vec<Script> {
        let mut result = vec![];
        let first_path = [ChildNumber::from(int_or_ext)];
        let first_deriv = self.xpub.derive_pub(&self.secp, &first_path).unwrap();

        let start = batch * BATCH_SIZE;
        let end = start + BATCH_SIZE;
        for j in start..end {
            let path = Path::new(int_or_ext, j);
            let p = self.get_script(&path).unwrap_or_else(|| {
                let second_path = [ChildNumber::from(j)];
                let second_deriv = first_deriv.derive_pub(&self.secp, &second_path).unwrap();
                //let address = Address::p2shwpkh(&second_deriv.public_key, Network::Testnet);
                let address = Address::p2wpkh(&second_deriv.public_key, Network::Testnet);
                let script = address.script_pubkey();
                self.insert_script(&path, &script);
                self.insert_path(&script, &path);
                script
            });
            result.push(p);
        }
        result
    }
}

struct Transactions(HashMap<Txid, Transaction>);
impl Default for Transactions {
    fn default() -> Self {
        Transactions(HashMap::new())
    }
}
impl Deref for Transactions {
    type Target = HashMap<Txid, Transaction>;
    fn deref(&self) -> &<Self as Deref>::Target {
        &self.0
    }
}
impl DerefMut for Transactions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Transactions {
    fn get_previous_output(&self, outpoint: &OutPoint) -> Option<TxOut> {
        self.0
            .get(&outpoint.txid)
            .map(|tx| tx.output[outpoint.vout as usize].clone())
    }
    fn get_previous_value(&self, outpoint: &OutPoint) -> Option<u64> {
        self.get_previous_output(outpoint).map(|o| o.value)
    }
}

fn main() {
    let xpub = "tpubD6NzVbkrYhZ4YRJLsDvYBaXK6EFi9MSV34h8BAvHPzW8RtUpJpBFiL23hRnvrRUcb6Fz9eKiVG8EzZudGXYfdo5tiP8BuhrsBmFAsREPZG4";
    let xpub = ExtendedPubKey::from_str(xpub).unwrap();
    let db = Forest::new("/tmp/db", xpub);

    sync(&db);
    let txs = list_tx(&db);
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
    let utxos = utxo(&db);
    let mut balance = 0u64;
    for utxo in utxos {
        println!("{} {}", utxo.0, utxo.1.value);
        balance += utxo.1.value;
    }
    println!("balance {}", balance);
}

fn sync(db: &Forest) {
    let start = Instant::now();

    let mut client = Client::new("tn.not.fyi:55001").unwrap();
    let mut history_txs_id = HashSet::new();
    let mut heights_set = HashSet::new();
    let mut txid_height = HashMap::new();

    for i in 0..=1 {
        let mut batch_count = 0;
        loop {
            let batch = db.get_script_batch(i, batch_count);
            let result: Vec<GetHistoryRes> = client
                .batch_script_get_history(&batch)
                .unwrap()
                .into_iter()
                .flatten()
                .collect();
            println!("{}/batch({}) {:?}", i, batch_count, result.len());

            if result.is_empty() {
                break;
            }

            for el in result {
                if el.height >= 0 {
                    heights_set.insert(el.height as u32);
                    txid_height.insert(el.tx_hash, el.height as u32);
                }
                history_txs_id.insert(el.tx_hash);
            }

            batch_count += 1;
        }
    }
    println!("elapsed {}", (Instant::now() - start).as_millis());

    let mut txs_to_download = Vec::new();
    for tx_id in history_txs_id.iter() {
        if db.get_tx(tx_id).is_none() {
            txs_to_download.push(tx_id);
        }
    }
    if !txs_to_download.is_empty() {
        let txs_downloaded = client.batch_transaction_get(txs_to_download).unwrap();
        println!(
            "txs_downloaded {:?} {}",
            txs_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
        let mut previous_txs_set = HashSet::new();
        for tx in txs_downloaded.iter() {
            db.insert_tx(&tx.txid(), &tx);
            for input in tx.input.iter() {
                previous_txs_set.insert(input.previous_output.txid);
            }
        }
        let mut previous_txs_vec = vec![];
        for tx_id in previous_txs_set {
            if db.get_tx(&tx_id).is_none() {
                previous_txs_vec.push(tx_id);
            }
        }
        let txs_downloaded = client.batch_transaction_get(&previous_txs_vec).unwrap();
        println!(
            "previous txs_downloaded {:?} {}",
            txs_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
        for tx in txs_downloaded.iter() {
            db.insert_tx(&tx.txid(), tx);
        }
    }

    let mut heights_to_download = Vec::new();
    for height in heights_set {
        if db.get_header(height).is_none() {
            heights_to_download.push(height);
        }
    }
    if !heights_to_download.is_empty() {
        let headers_downloaded = client
            .batch_block_header(heights_to_download.clone())
            .unwrap();
        for (header, height) in headers_downloaded.iter().zip(heights_to_download.iter()) {
            db.insert_header(*height, header);
        }
        println!(
            "headers_downloaded {:?} {}",
            headers_downloaded.len(),
            (Instant::now() - start).as_millis()
        );
    }

    // sync heights, which are my txs
    for (txid, height) in txid_height.iter() {
        db.insert_height(txid, *height); // adding new, but also updating reorged tx
    }
    for (txid_db, _) in db.get_heights().iter() {
        if txid_height.get(txid_db).is_none() {
            db.remove_height(txid_db); // something in the db is not in live list (rbf), removing
        }
    }

    println!("elapsed {}", (Instant::now() - start).as_millis());
}

struct TransactionMeta {
    tx: Transaction,
    fee: u64,
    my_in: u64,
    my_out: u64,
    height: Option<u32>,
    time: Option<u32>,
}

fn list_tx(db: &Forest) -> Vec<TransactionMeta> {
    let (_, all_txs) = db.get_all_spent_and_txs();
    let heights = db.get_heights();
    let mut txs = vec![];

    for (tx_id, height) in heights {
        let tx = all_txs.get(&tx_id).unwrap();
        let header = height.map(|h| db.get_header(h).unwrap());
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
    txs.sort_by(|a, b| b.time.unwrap_or(std::u32::MAX).cmp(&a.time.unwrap_or(std::u32::MAX))
    );
    txs
}

fn utxo(db: &Forest) -> Vec<(OutPoint, TxOut)> {
    let (spent, all_txs) = db.get_all_spent_and_txs();
    let heights = db.get_heights();
    let mut utxos = vec![];
    for (tx_id, _) in heights {
        let tx = all_txs.get(&tx_id).unwrap();

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
    utxos
}

struct Height([u8; 4], u32);
impl AsRef<[u8]> for Height {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl Height {
    fn new(height: u32) -> Self {
        Height(height.to_be_bytes(), height)
    }
    fn from_slice(slice: &[u8]) -> Self {
        let i: [u8; 4] = slice[..].try_into().unwrap();
        Height(i, u32::from_be_bytes(i))
    }
}

//DerivationPath hasn't AsRef<[u8]>
#[derive(Debug, PartialEq)]
struct Path {
    bytes: [u8; 8],
    pub i: u32,
    pub j: u32,
}
impl AsRef<[u8]> for Path {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Path {
    fn new(i: u32, j: u32) -> Self {
        let value = ((i as u64) << 32) + j as u64;
        let bytes = value.to_be_bytes();
        Path { bytes, i, j }
    }

    fn from_slice(slice: &[u8]) -> Self {
        let i: [u8; 4] = slice[..4].try_into().unwrap();
        let j: [u8; 4] = slice[4..].try_into().unwrap();

        Path::new(u32::from_be_bytes(i), u32::from_be_bytes(j))
    }
}

#[cfg(test)]
mod test {
    use crate::Path;

    #[test]
    fn test_path() {
        let path = Path::new(0, 0);
        assert_eq!(path, Path::from_slice(path.as_ref()));
        let path = Path::new(0, 220);
        assert_eq!(path, Path::from_slice(path.as_ref()));
        let path = Path::new(1, 220);
        assert_eq!(path, Path::from_slice(path.as_ref()));
        let path = Path::new(1, 0);
        assert_eq!(path, Path::from_slice(path.as_ref()));
    }
}
