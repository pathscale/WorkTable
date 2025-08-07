// use eyre::bail;
// use rkyv::{Archive, Serialize};
// use uuid::timestamp;
// use worktable::prelude::{MemStat, SizeMeasurable};
//
// use std::{collections::HashMap, sync::Arc};
//
// use tokio::sync::RwLock;
// use worktable::{select::SelectQueryExecutor, WorkTableError};
//
// use atomic_float::AtomicF64;
// use futures::executor::block_on;
// use rand::distr::Alphanumeric;
// use rand::Rng;
// use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
// use std::time::{SystemTime, UNIX_EPOCH};
// use worktable::prelude::*;
// use worktable::worktable;
//
// #[derive(
//     Archive,
//     Clone,
//     Copy,
//     Debug,
//     Default,
//     serde::Deserialize,
//     rkyv::Deserialize,
//     Eq,
//     Hash,
//     MemStat,
//     Ord,
//     Serialize,
//     PartialEq,
//     PartialOrd,
// )]
// #[rkyv(compare(PartialEq), derive(Debug))]
// pub enum Exchange {
//     #[default]
//     Unset,
//     BinanceFutures,
//     BinanceSpot,
//     HyperliquidPerpetuals,
//     HyperliquidSpot,
//     GateioSpot,
//     GateioFutures,
//     KucoinSpot,
//     KucoinFutures,
//     BybitSpot,
//     BybitFutures,
//     BsxFutures,
//     BitgetSpot,
//     BitgetFutures,
//     BitmexSpot,
//     VertexSpot,
//     VertexFutures,
//     OKXSpot,
//     OKXFutures,
//     DeribitFutures,
// }
//
// impl SizeMeasurable for Exchange {
//     fn aligned_size(&self) -> usize {
//         size_of::<Exchange>()
//     }
// }
//
// impl Exchange {
//     pub const TOTAL: usize = 19;
//
//     pub fn is_hyper(&self) -> bool {
//         *self == Exchange::HyperliquidPerpetuals
//     }
//
//     pub fn is_left(&self) -> bool {
//         *self == Exchange::BinanceFutures
//     }
//
//     pub fn is_right(&self) -> bool {
//         *self == Exchange::HyperliquidPerpetuals
//     }
//
//     pub fn all() -> [Self; Self::TOTAL] {
//         [
//             Self::BinanceFutures,
//             Self::BinanceSpot,
//             Self::HyperliquidPerpetuals,
//             Self::HyperliquidSpot,
//             Self::GateioSpot,
//             Self::GateioFutures,
//             Self::KucoinSpot,
//             Self::KucoinFutures,
//             Self::BybitSpot,
//             Self::BybitFutures,
//             Self::BsxFutures,
//             Self::BitgetSpot,
//             Self::BitgetFutures,
//             Self::BitmexSpot,
//             Self::VertexSpot,
//             Self::VertexFutures,
//             Self::OKXSpot,
//             Self::OKXFutures,
//             Self::DeribitFutures,
//         ]
//     }
//
//     pub const fn as_str(&self) -> &'static str {
//         match self {
//             Exchange::Unset => "Unset",
//             Exchange::BinanceFutures => "BIN-futures",
//             Exchange::BinanceSpot => "BIN-spot",
//             Exchange::HyperliquidPerpetuals => "HYP-perps",
//             Exchange::HyperliquidSpot => "HYP-spot",
//             Exchange::GateioSpot => "GAT-spot",
//             Exchange::GateioFutures => "GAT-futures",
//             Exchange::KucoinSpot => "KUC-spot",
//             Exchange::KucoinFutures => "KUC-futures",
//             Exchange::BybitSpot => "BYB-spot",
//             Exchange::BybitFutures => "BYB-futures",
//             Exchange::BsxFutures => "BSX-futures",
//             Exchange::BitgetSpot => "BGT-spot",
//             Exchange::BitgetFutures => "BGT-futures",
//             Exchange::BitmexSpot => "BMX-spot",
//             Exchange::VertexSpot => "VTX-spot",
//             Exchange::VertexFutures => "VTX-futures",
//             Exchange::OKXSpot => "OKX-spot",
//             Exchange::OKXFutures => "OKX-futures",
//             Exchange::DeribitFutures => "DBT-futures",
//         }
//     }
// }
//
// impl std::fmt::Display for Exchange {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str(self.as_str())
//     }
// }
//
// impl AsRef<str> for Exchange {
//     fn as_ref(&self) -> &str {
//         self.as_str()
//     }
// }
//
// worktable!(
//     name: Spread,
//     columns: {
//         id: u32 primary_key autoincrement,
//         left_timestamp: u64,
//         right_timestamp: u64,
//         left_exchange: Exchange,
//         right_exchange: Exchange,
//         left_price: f64,
//         right_price: f64,
//         spread: f64,
//         symbol: String,
//         key: String,
//         left_key: String,
//         right_key: String,
//     },
//     indexes: {
//         key_idx: key unique,
//         symbol_idx: symbol,
//         left_key_idx: left_key,
//         right_key_idx: right_key
//     },
//     queries: {
//         in_place: {
//             Left(left_price, left_timestamp, spread) by id,
//             Right(right_price, right_timestamp, spread) by id,
//         }
//         update: {
//             ExchangeLeft(left_exchange, left_key, key) by id,
//             ExchangeRight(right_exchange, right_key, key) by id,
//         }
//     }
// );
//
// #[derive(Debug)]
// struct MedianBucket {
//     ts: AtomicU64,
//     idx: AtomicUsize,
//     values: RwLock<Vec<AtomicF64>>,
// }
//
// impl MedianBucket {
//     async fn add(&self, now: u64, value: f64) {
//         let prev_ts = self.ts.load(Ordering::Acquire);
//
//         if prev_ts != now
//             && self
//                 .ts
//                 .compare_exchange(prev_ts, now, Ordering::AcqRel, Ordering::Acquire)
//                 .is_ok()
//         {
//             let values = self.values.write().await;
//             for val in values.iter() {
//                 val.store(f64::NAN, Ordering::Relaxed);
//             }
//             self.idx.store(0, Ordering::Relaxed);
//         }
//
//         let values = self.values.read().await;
//         let idx = self.idx.fetch_add(1, Ordering::Relaxed);
//
//         if idx >= values.len() {
//             drop(values);
//             let mut values = self.values.write().await;
//             values.push(AtomicF64::new(value));
//         } else {
//             values[idx].store(value, Ordering::Relaxed);
//         }
//     }
//
//     async fn collect(&self, cutoff: u64) -> Option<Vec<f64>> {
//         if self.ts.load(Ordering::Acquire) <= cutoff {
//             return None;
//         }
//
//         let values = self.values.read().await;
//         Some(
//             values
//                 .iter()
//                 .filter_map(|v| {
//                     let x = v.load(Ordering::Relaxed);
//                     if x.is_nan() {
//                         None
//                     } else {
//                         Some(x)
//                     }
//                 })
//                 .collect(),
//         )
//     }
// }
//
// #[derive(Debug)]
// struct Bucket {
//     ts: AtomicU64,
//     sum: AtomicF64,
//     count: AtomicUsize,
// }
//
// #[derive(Debug)]
// struct Window {
//     secs: u64,
//     start: AtomicU64,
//     avg_buckets: Box<[Bucket]>,
//     med_buckets: Box<[MedianBucket]>,
// }
//
// impl Window {
//     fn new(secs: u64) -> Self {
//         let mut avg_vec = Vec::with_capacity(secs as usize);
//         let mut med_vec = Vec::with_capacity(secs as usize);
//         for _ in 0..secs {
//             avg_vec.push(Bucket {
//                 ts: AtomicU64::new(0),
//                 sum: AtomicF64::new(0.0),
//                 count: AtomicUsize::new(0),
//             });
//             med_vec.push(MedianBucket {
//                 ts: AtomicU64::new(0),
//                 values: RwLock::new(Vec::new()),
//                 idx: AtomicUsize::new(0),
//             });
//         }
//         Self {
//             secs,
//             start: AtomicU64::new(0),
//             avg_buckets: avg_vec.into_boxed_slice(),
//             med_buckets: med_vec.into_boxed_slice(),
//         }
//     }
//
//     async fn add(&self, value: f64) {
//         let now = current_epoch_seconds();
//         _ = self
//             .start
//             .compare_exchange(0, now, Ordering::Release, Ordering::Relaxed);
//
//         let Ok(idx) = usize::try_from(now % self.secs) else {
//             return;
//         };
//
//         let avg_bucket = &self.avg_buckets[idx];
//         let prev_ts = avg_bucket.ts.load(Ordering::Acquire);
//         if prev_ts != now
//             && avg_bucket
//                 .ts
//                 .compare_exchange(prev_ts, now, Ordering::AcqRel, Ordering::Acquire)
//                 .is_ok()
//         {
//             avg_bucket.sum.store(0.0, Ordering::Relaxed);
//             avg_bucket.count.store(0, Ordering::Relaxed);
//         }
//         avg_bucket.sum.fetch_add(value, Ordering::Relaxed);
//         avg_bucket.count.fetch_add(1, Ordering::Relaxed);
//
//         let med_bucket = &self.med_buckets[idx];
//         med_bucket.add(now, value).await;
//     }
//
//     fn average(&self) -> Option<f64> {
//         let now = current_epoch_seconds();
//         let start = self.start.load(Ordering::Acquire);
//         if start == 0 || now < start.saturating_add(self.secs) {
//             return None;
//         }
//
//         let cutoff = now.saturating_sub(self.secs);
//         let mut total = 0.0;
//         let mut cnt = 0usize;
//
//         for bucket in self.avg_buckets.iter() {
//             let ts = bucket.ts.load(Ordering::Acquire);
//             if ts > cutoff {
//                 total += bucket.sum.load(Ordering::Relaxed);
//                 cnt += bucket.count.load(Ordering::Relaxed);
//             }
//         }
//
//         if cnt == 0 {
//             None
//         } else {
//             Some(total / (cnt as f64))
//         }
//     }
//
//     async fn median(&self) -> Option<f64> {
//         let now = current_epoch_seconds();
//         let cutoff = now.saturating_sub(self.secs);
//
//         let mut all_values = Vec::new();
//
//         for bucket in self.med_buckets.iter() {
//             if let Some(mut vals) = bucket.collect(cutoff).await {
//                 all_values.append(&mut vals);
//             }
//         }
//
//         if all_values.is_empty() {
//             return None;
//         }
//
//         all_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
//         let mid = all_values.len() / 2;
//         Some(if all_values.len() % 2 == 0 {
//             (all_values[mid - 1] + all_values[mid]) / 2.0
//         } else {
//             all_values[mid]
//         })
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct SpreadWindow {
//     secs: u64,
//     windows: Arc<RwLock<HashMap<String, Window>>>,
// }
//
// impl SpreadWindow {
//     pub fn new(secs: u64) -> Self {
//         Self {
//             secs,
//             windows: Arc::new(RwLock::new(HashMap::new())),
//         }
//     }
//
//     pub async fn add(&self, symbol: &str, spread: f64) {
//         let guard = self.windows.read().await;
//         if let Some(window) = guard.get(symbol) {
//             window.add(spread).await;
//             return;
//         }
//         drop(guard);
//         let mut guard = self.windows.write().await;
//         let window = Window::new(self.secs);
//         window.add(spread).await;
//         guard.insert(symbol.to_string(), window);
//     }
//
//     pub async fn average(&self, symbol: &str) -> Option<f64> {
//         let guard = self.windows.read().await;
//         guard.get(symbol)?.average()
//     }
//
//     pub async fn median(&self, symbol: &str) -> Option<f64> {
//         let guard = self.windows.read().await;
//         guard.get(symbol)?.median().await
//     }
// }
//
// fn current_epoch_seconds() -> u64 {
//     SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .expect("system time before UNIX EPOCH")
//         .as_secs()
// }
//
// fn get_basis_point(operand: f64, comparator: f64) -> f64 {
//     let left = operand - comparator;
//     let right = (operand + comparator) / 2.0;
//     (left / right) * 10_000.0
// }
//
// pub struct SpreadManager {
//     spread_wt: Arc<SpreadWorkTable>,
//     windows: Arc<RwLock<HashMap<(Exchange, Exchange), SpreadWindow>>>,
// }
//
// impl SpreadManager {
//     pub fn new(spread_wt: Arc<SpreadWorkTable>) -> Self {
//         Self {
//             spread_wt,
//             windows: Arc::new(RwLock::new(HashMap::new())),
//         }
//     }
//
//     pub async fn record(
//         &self,
//         exchange: Exchange,
//         symbol: &str,
//         ask_price: Option<f64>,
//         bid_price: Option<f64>,
//         timestamp: u64,
//     ) -> eyre::Result<()> {
//         let key = format!("{}-{}", symbol, exchange);
//         let left = self.spread_wt.select_by_left_key(key.clone()).execute()?;
//
//         if left.is_empty() {
//             let id = self.spread_wt.get_next_pk().into();
//             if let Err(e) = self.spread_wt.insert(SpreadRow {
//                 id,
//                 key: format!("unset-left-{key}"),
//                 symbol: symbol.to_string(),
//                 left_timestamp: timestamp,
//                 left_price: bid_price.unwrap_or_default(),
//                 left_exchange: exchange,
//                 left_key: key.clone(),
//                 spread: 0.0,
//                 right_exchange: Exchange::Unset,
//                 right_key: String::new(),
//                 right_price: 0.0,
//                 right_timestamp: 0,
//             }) {
//                 if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                     println!("failed to write unset left spread: {e}");
//                     return Err(e.into());
//                 }
//             };
//         }
//         let right = self.spread_wt.select_by_right_key(key.clone()).execute()?;
//         if right.is_empty() {
//             let id = self.spread_wt.get_next_pk().into();
//             if let Err(e) = self.spread_wt.insert(SpreadRow {
//                 id,
//                 key: format!("unset-right-{key}"),
//                 symbol: symbol.to_string(),
//                 right_timestamp: timestamp,
//                 right_price: ask_price.unwrap_or_default(),
//                 left_price: 0.0,
//                 right_exchange: exchange,
//                 right_key: key.clone(),
//                 spread: 0.0,
//                 left_exchange: Exchange::Unset,
//                 left_key: String::new(),
//                 left_timestamp: 0,
//             }) {
//                 if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                     println!("failed to write unset right spread: {e}");
//                     return Err(e.into());
//                 }
//             };
//         }
//
//         let spreads = self
//             .spread_wt
//             .select_by_symbol(symbol.to_string())
//             .execute()?;
//
//         for spread in spreads {
//             if spread.left_exchange == Exchange::Unset {
//                 if spread.right_exchange == exchange {
//                     continue;
//                 }
//                 let query = ExchangeLeftQuery {
//                     left_exchange: exchange,
//                     left_key: key.clone(),
//                     key: format!("{}-{}", key, spread.right_key),
//                 };
//                 let mut unset = spread.clone();
//                 unset.id = self.spread_wt.get_next_pk().into();
//                 if let Err(e) = self.spread_wt.update_exchange_left(query, spread.id).await {
//                     if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                         println!("failed to update left spread exchange: {e}");
//                         return Err(e.into());
//                     }
//                 } else {
//                     self.windows
//                         .write()
//                         .await
//                         .insert((exchange, spread.right_exchange), SpreadWindow::new(15));
//                 }
//                 if let Err(e) = self.spread_wt.insert(unset.clone()) {
//                     if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                         println!("failed to copy unset left spread: {e}, {unset:?}");
//                         return Err(e.into());
//                     }
//                 };
//             }
//
//             if spread.right_exchange == Exchange::Unset {
//                 if spread.left_exchange == exchange {
//                     continue;
//                 }
//                 let query = ExchangeRightQuery {
//                     right_exchange: exchange,
//                     right_key: key.clone(),
//                     key: format!("{}-{}", spread.left_key, key),
//                 };
//                 let mut unset = spread.clone();
//                 unset.id = self.spread_wt.get_next_pk().into();
//                 if let Err(e) = self.spread_wt.update_exchange_right(query, spread.id).await {
//                     if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                         println!("failed to update right spread exchange: {e}");
//                         return Err(e.into());
//                     }
//                 } else {
//                     self.windows
//                         .write()
//                         .await
//                         .insert((spread.left_exchange, exchange), SpreadWindow::new(15));
//                 }
//                 if let Err(e) = self.spread_wt.insert(unset.clone()) {
//                     if !matches!(e, WorkTableError::AlreadyExists(..)) {
//                         println!("failed to copy unset right spread: {e}, {unset:?}");
//                         return Err(e.into());
//                     }
//                 };
//             }
//         }
//
//         let spreads = self
//             .spread_wt
//             .select_by_symbol(symbol.to_string())
//             .execute()?;
//
//         for spread in spreads {
//             if spread.right_exchange == Exchange::Unset || spread.left_exchange == Exchange::Unset {
//                 continue;
//             }
//             let value = if spread.left_exchange == exchange {
//                 let bid_price = bid_price.unwrap_or(spread.left_price);
//                 let value = get_basis_point(bid_price, spread.right_price);
//
//                 if let Err(e) = self
//                     .spread_wt
//                     .update_left_in_place(
//                         |x| {
//                             let (left_price, left_timestamp, spread) = x;
//                             *left_timestamp = timestamp.into();
//                             *left_price = bid_price.into();
//                             *spread = value.into();
//                         },
//                         spread.id,
//                     )
//                     .await
//                 {
//                     println!("failed to update left spread: {e}");
//                     return Err(e);
//                 };
//                 value
//             } else if spread.right_exchange == exchange {
//                 let ask_price = ask_price.unwrap_or(spread.right_price);
//                 let value = get_basis_point(spread.left_price, ask_price);
//
//                 if let Err(e) = self
//                     .spread_wt
//                     .update_right_in_place(
//                         |x| {
//                             let (right_price, right_timestamp, spread) = x;
//                             *right_timestamp = timestamp.into();
//                             *right_price = ask_price.into();
//                             *spread = value.into();
//                         },
//                         spread.id,
//                     )
//                     .await
//                 {
//                     {
//                         println!("failed to update right spread: {e}");
//                         return Err(e);
//                     };
//                 };
//                 value
//             } else {
//                 continue;
//             };
//
//             self.windows
//                 .read()
//                 .await
//                 .get(&(spread.left_exchange, spread.right_exchange))
//                 .expect("window should exist")
//                 .add(symbol, value)
//                 .await;
//         }
//
//         Ok(())
//     }
//
//     pub async fn median(
//         &self,
//         left_exchange: Exchange,
//         right_exchange: Exchange,
//         symbol: &str,
//     ) -> Option<f64> {
//         self.windows
//             .read()
//             .await
//             .get(&(left_exchange, right_exchange))?
//             .median(symbol)
//             .await
//     }
// }
//
// #[tokio::main(flavor = "multi_thread", worker_threads = 16)]
// async fn main() {
//     let spread_wt = Arc::new(SpreadWorkTable::default());
//
//     let manager = Arc::new(SpreadManager::new(spread_wt.clone()));
//
//     let mut symbols = Vec::new();
//     for _ in 0..5000 {
//         let s: String = rand::rng()
//             .sample_iter(&Alphanumeric)
//             .take(7)
//             .map(char::from)
//             .collect();
//         symbols.push(s);
//     }
//     symbols.push(String::new());
//
//     let symbols = Arc::new(symbols);
//
//     let ts_seq = Arc::new(AtomicU64::new(0));
//     let ask_seq = Arc::new(AtomicF64::new(0.0));
//     let bid_seq = Arc::new(AtomicF64::new(0.0));
//
//     let mut seq = Arc::new(AtomicUsize::new(0));
//     let mut handles = Vec::new();
//
//     for i in 0..16 {
//         let ask_seq = ask_seq.clone();
//         let bid_seq = bid_seq.clone();
//         let ts_seq = ts_seq.clone();
//         let manager = manager.clone();
//         let seq = seq.clone();
//         let symbols = symbols.clone();
//         handles.push(tokio::spawn(async move {
//             for _ in 0..500_000 {
//                 let timestamp = ts_seq.fetch_add(12, Ordering::Relaxed);
//                 let idx = seq.fetch_add(1, Ordering::Relaxed) % 5000;
//                 let symbol = symbols[idx].clone();
//                 let exchange = match idx % 3 {
//                     0 => Exchange::HyperliquidPerpetuals,
//                     1 => Exchange::BinanceFutures,
//                     _ => Exchange::BybitFutures,
//                 };
//                 let ask_price = ask_seq.fetch_add(1.0, Ordering::Relaxed);
//                 let bid_price = bid_seq.fetch_add(10.0, Ordering::Relaxed);
//                 // println!("record {exchange} {ask_price} {bid_price}");
//                 if let Err(e) = manager
//                     .record(
//                         exchange,
//                         &symbol,
//                         Some(ask_price),
//                         Some(bid_price),
//                         timestamp,
//                     )
//                     .await
//                 {
//                     println!("failed to record spread: {e:?}");
//                 };
//             }
//         }));
//     }
//
//     for handle in handles {
//         println!("{:?}", handle.await);
//     }
// }

fn main() {}
