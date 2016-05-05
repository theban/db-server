extern crate theban_db;
extern crate memrange;

//use self::rustc_serialize::{Encodable,Decodable};

use self::memrange::Range;
use theban_db::DB;
use theban_db::Bitmap;
use dberror::DBServerError;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::Arc;

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub struct WriteAccess{
    pub rng: Range,
    pub val: Vec<u8>,
}

impl WriteAccess{
    pub fn new(rng: Range, val: Vec<u8>) -> Self{
        return WriteAccess{rng: rng, val: val}
    }
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub enum DBInstruction {
    OGet(String, Vec<Range>),
    OPut(String, Vec<WriteAccess>),
    ODel(String, Vec<Range>),
    ODelAll(String, Vec<Range>),
    BGet(String, Vec<Range>),
    BPut(String, Vec<WriteAccess>), // table, datasize, data
    BDel(String, Vec<Range>),
    Save(String),
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub enum DBAnswer {
    Done,
    Tags(HashMap<Range,Vec<(Range,Vec<u8>)> >),
    Bitmap(HashMap<Range, Vec<(Range,Bitmap)> >),
}

fn get_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBServerError> {
    let mut data = HashMap::new();
    let db_access = try!(db.read());
    for rng in rngs {
        let mut cur_query_res = vec![];
        if let Some(iter) = db_access.query_object(&table, rng) {
            for (r,v) in iter {
                cur_query_res.push((r, v.data.clone()))
            }
            data.insert(rng, cur_query_res);
        }
    }
    return Ok(DBAnswer::Tags(data))
}

fn put_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<WriteAccess>) -> Result<DBAnswer, DBServerError> {
    let mut db_access = try!(db.write());
    for w in rngs {
        db_access.insert_object(&table, w.rng, theban_db::Object::new(w.val))
    }
    return Ok(DBAnswer::Done)
}

fn del_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBServerError> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_object(&table, rng)
    }
    return Ok(DBAnswer::Done)
}

fn delall_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBServerError> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_intersecting_objects(&table, rng)
    }
    return Ok(DBAnswer::Done)
}

fn get_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBServerError> {
    let mut data = HashMap::new();
    let db_access = try!(db.read());
    for rng in rngs {
        let mut cur_query_res = vec![];
        if let Some(iter) = db_access.query_bitmap(&table, rng) {
            for (r,v) in iter {
                cur_query_res.push((r, Bitmap::new(v.entry_size, v.data.to_vec())));
            }
            data.insert(rng, cur_query_res);
        }
    }
    return Ok(DBAnswer::Bitmap(data))
}

fn put_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String,  rngs: Vec<WriteAccess>) -> Result<DBAnswer, DBServerError> {
    let mut db_access = try!(db.write());
    for w in rngs {
        db_access.insert_bitmap(&table, w.rng, theban_db::Bitmap::new(1, w.val))
    }
    return Ok(DBAnswer::Done)
}

fn del_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBServerError> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_bitmap(&table, 1, rng)
    }
    return Ok(DBAnswer::Done)
}

fn save_to_file<'a>(db: Arc<RwLock<Box<DB>>>, file: &String) -> Result<DBAnswer, DBServerError>{
    let db_access = try!(db.read());
    try!(db_access.save_to_file(file));
    return Ok(DBAnswer::Done)
}


pub fn execute_db_instruction<'a>(db: Arc<RwLock<Box<DB>>>, instr: DBInstruction) -> Result<DBAnswer, DBServerError>{
    match instr {
        DBInstruction::OGet(table, ranges) => get_query(db, table, ranges),
        DBInstruction::OPut(table, access) => put_query(db, table, access),
        DBInstruction::ODel(table, ranges) => del_query(db, table, ranges),
        DBInstruction::ODelAll(table, ranges)   => delall_query(db, table, ranges),
        DBInstruction::BGet(table, ranges)      => get_bquery(db, table, ranges),
        DBInstruction::BPut(table,  access) => put_bquery(db, table, access),
        DBInstruction::BDel(table,  ranges) => del_bquery(db, table, ranges),
        DBInstruction::Save(file) => save_to_file(db, &file),
    }
}
