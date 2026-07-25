//! The "dynamic twin" for the specialization ablation (paper Table 1).
//!
//! Models what a dynamic-schema engine is forced to do on every operation:
//!   * rows are vectors of tagged values (boxed representation)
//!   * a runtime catalog maps column name -> position (hash lookup per access)
//!   * rows are encoded/decoded through a tag-dispatched serializer per access
//!   * row locking is a coarse per-row mutex from a dynamic lock table
//!
//! FAIRNESS NOTE (review before trusting numbers): this v1 twin does NOT share
//! WorkTable's page allocator or B-tree; it uses a slot vector + BTreeMap.
//! That is *favorable* to the twin for point ops (no page indirection), so a
//! win for the specialized engine here is conservative. A v2 twin sharing the
//! page/index machinery with only the row representation dynamized would
//! isolate the typing cost even more precisely.

use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    U64(u64),
    F64(f64),
    Str(String),
}

pub struct DynSchema {
    pub cols: Vec<&'static str>,
    pub catalog: HashMap<&'static str, usize>,
}

impl DynSchema {
    pub fn bench() -> Self {
        let cols = vec!["id", "a", "b", "e", "c", "d"];
        let catalog = cols.iter().enumerate().map(|(i, c)| (*c, i)).collect();
        DynSchema { cols, catalog }
    }
}

fn encode(row: &[Value], out: &mut Vec<u8>) {
    out.clear();
    for v in row {
        match v {
            Value::U64(x) => {
                out.push(0);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Value::F64(x) => {
                out.push(1);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Value::Str(s) => {
                out.push(2);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
}

fn decode(mut b: &[u8]) -> Vec<Value> {
    let mut row = Vec::with_capacity(6);
    while !b.is_empty() {
        match b[0] {
            0 => {
                row.push(Value::U64(u64::from_le_bytes(b[1..9].try_into().unwrap())));
                b = &b[9..];
            }
            1 => {
                row.push(Value::F64(f64::from_le_bytes(b[1..9].try_into().unwrap())));
                b = &b[9..];
            }
            2 => {
                let n = u32::from_le_bytes(b[1..5].try_into().unwrap()) as usize;
                row.push(Value::Str(String::from_utf8(b[5..5 + n].to_vec()).unwrap()));
                b = &b[5 + n..];
            }
            _ => unreachable!(),
        }
    }
    row
}

pub struct DynTable {
    pub schema: DynSchema,
    slots: RwLock<Vec<Vec<u8>>>,
    pk_index: RwLock<BTreeMap<u64, usize>>,
    locks: RwLock<HashMap<u64, Arc<Mutex<()>>>>,
    next_pk: AtomicU64,
}

impl DynTable {
    pub fn new() -> Self {
        DynTable {
            schema: DynSchema::bench(),
            slots: RwLock::new(Vec::new()),
            pk_index: RwLock::new(BTreeMap::new()),
            locks: RwLock::new(HashMap::new()),
            next_pk: AtomicU64::new(0),
        }
    }

    pub fn get_next_pk(&self) -> u64 {
        self.next_pk.fetch_add(1, Ordering::AcqRel)
    }

    pub fn insert(&self, row: Vec<Value>) -> u64 {
        let pk = match &row[0] {
            Value::U64(x) => *x,
            _ => panic!("pk must be u64"),
        };
        let mut buf = Vec::with_capacity(64);
        encode(&row, &mut buf);
        let mut slots = self.slots.write();
        let idx = slots.len();
        slots.push(buf);
        drop(slots);
        self.pk_index.write().insert(pk, idx);
        pk
    }

    pub fn select(&self, pk: u64) -> Option<Vec<Value>> {
        let idx = *self.pk_index.read().get(&pk)?;
        let slots = self.slots.read();
        Some(decode(&slots[idx]))
    }

    /// Field update through the catalog — the dynamic path a specialized
    /// `update_upd_b` avoids: lock table, hash lookup, decode, dispatch,
    /// re-encode, write back.
    pub fn update_field(&self, pk: u64, col: &str, v: Value) -> Option<()> {
        let lock = {
            let mut locks = self.locks.write();
            locks.entry(pk).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
        };
        let _g = lock.lock();
        let idx = *self.pk_index.read().get(&pk)?;
        let pos = *self.schema.catalog.get(col)?;
        let mut slots = self.slots.write();
        let mut row = decode(&slots[idx]);
        row[pos] = v;
        let mut buf = Vec::with_capacity(64);
        encode(&row, &mut buf);
        slots[idx] = buf;
        Some(())
    }
}

pub fn mk_dyn_row(pk: u64, v: u64) -> Vec<Value> {
    vec![
        Value::U64(pk),
        Value::U64(v),
        Value::U64(v),
        Value::U64(v),
        Value::F64(v as f64),
        Value::Str("payloadpayload".to_string()),
    ]
}
