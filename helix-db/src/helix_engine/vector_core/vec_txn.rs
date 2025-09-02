use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
};

use heed3::{Env, RoTxn, RwTxn};
use lru::LruCache;

pub struct VecTxn<'a> {
    pub txn: &'a mut RwTxn<'a>,
    pub cache: Option<HashMap<u128, HashSet<u128>>>,
}

impl<'a> VecTxn<'a> {
    pub fn new(txn: &'a mut RwTxn<'a>) -> Self {
        Self {
            txn,
            // lru_cache: Some(LruCache::new(
            //     NonZeroUsize::new(8192).expect("2048 is a valid non-zero size"),
            // )),
            cache: Some(HashMap::with_capacity(2048)),
        }
    }
}


