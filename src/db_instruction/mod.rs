extern crate theban_db;
extern crate memrange;

use self::memrange::Range;
use theban_db::DB;
use theban_db::Bitmap;
use dberror::DBError;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct WriteAccess{
    pub rng: Range,
    pub val: Vec<u8>,
}


#[derive(Debug)]
pub enum DBInstructionType {

    TOGET = 1, //operations on objects
    TOPUT = 2,
    TODEL = 3,
    TODELALL = 4,

    TBGET = 5, //operations on bitmaps
    TBPUT = 6,
    TBDEL = 7,

    TSAVE = 8,
    TLAST = 9,
}

pub fn instr_from_opcode<'a>(opcode : u64) -> Result<DBInstructionType,DBError<'a>>{
    if opcode >= DBInstructionType::TLAST as u64 {return Err(DBError::ProtocolError("instruction type unknown".into()))}
    match opcode {
        1 => Ok(DBInstructionType::TOGET),
        2 => Ok(DBInstructionType::TOPUT),
        3 => Ok(DBInstructionType::TODEL),
        4 => Ok(DBInstructionType::TODELALL),

        5 => Ok(DBInstructionType::TBGET),
        6 => Ok(DBInstructionType::TBPUT),
        7 => Ok(DBInstructionType::TBDEL),

        8 => Ok(DBInstructionType::TSAVE),
        9 => Ok(DBInstructionType::TLAST),
        _ => unreachable!(),
    }
}

#[derive(Debug)]
pub enum DBInstruction {
    OGet(String, Vec<Range>),
    OPut(String, Vec<WriteAccess>),
    ODel(String, Vec<Range>),
    ODelAll(String, Vec<Range>),
    BGet(String, Vec<Range>),
    BPut(String, u64, Vec<WriteAccess>), // table, datasize, data
    BDel(String, u64, Vec<Range>),
    Save(String),
}

#[derive(Debug)]
pub enum DBAnswer {
    Done,
    Tags(HashMap<Range,Vec<u8>>),
    Bitmap(HashMap<Range, Bitmap>),
}

fn get_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBError<'a>> {
    let mut data = HashMap::new();
    let db_access = try!(db.read());
    for rng in rngs {
        if let Some(iter) = db_access.query_object(&table, rng) {
            for (r,v) in iter {
                data.insert(r.clone(), v.data.clone());
            }
        }
    }
    return Ok(DBAnswer::Tags(data))
}

fn put_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<WriteAccess>) -> Result<DBAnswer, DBError<'a>> {
    let mut db_access = try!(db.write());
    for w in rngs {
        db_access.insert_object(&table, w.rng, theban_db::Object::new(w.val))
    }
    return Ok(DBAnswer::Done)
}

fn del_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBError<'a>> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_object(&table, rng)
    }
    return Ok(DBAnswer::Done)
}

fn delall_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBError<'a>> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_intersecting_objects(&table, rng)
    }
    return Ok(DBAnswer::Done)
}

fn get_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBAnswer, DBError<'a>> {
    let mut data = HashMap::new();
    let db_access = try!(db.read());
    for rng in rngs {
        if let Some(iter) = db_access.query_bitmap(&table, rng) {
            for (r,v) in iter {
                data.insert(r.clone(), Bitmap::new(v.entry_size,v.data.to_vec()));
            }
        }
    }
    return Ok(DBAnswer::Bitmap(data))
}

fn put_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String, entry_size: u64, rngs: Vec<WriteAccess>) -> Result<DBAnswer, DBError<'a>> {
    let mut db_access = try!(db.write());
    for w in rngs {
        db_access.insert_bitmap(&table, w.rng, theban_db::Bitmap::new(entry_size, w.val))
    }
    return Ok(DBAnswer::Done)
}

fn del_bquery<'a>(db: Arc<RwLock<Box<DB>>>, table: String, entry_size: u64, rngs: Vec<Range>) -> Result<DBAnswer, DBError<'a>> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_bitmap(&table, entry_size, rng)
    }
    return Ok(DBAnswer::Done)
}

fn save_to_file<'a>(db: Arc<RwLock<Box<DB>>>, file: &String) -> Result<DBAnswer, DBError<'a>>{
    let db_access = try!(db.read());
    try!(db_access.save_to_file(file));
    return Ok(DBAnswer::Done)
}


pub fn execute_db_instruction<'a>(db: Arc<RwLock<Box<DB>>>, instr: DBInstruction) -> Result<DBAnswer, DBError<'a>>{
    match instr {
        DBInstruction::OGet(table, ranges) => get_query(db, table, ranges),
        DBInstruction::OPut(table, access) => put_query(db, table, access),
        DBInstruction::ODel(table, ranges) => del_query(db, table, ranges),
        DBInstruction::ODelAll(table, ranges) => delall_query(db, table, ranges),
        DBInstruction::BGet(table, ranges) => get_bquery(db, table, ranges),
        DBInstruction::BPut(table, entry_size, access) => put_bquery(db, table, entry_size, access),
        DBInstruction::BDel(table, entry_size, ranges) => del_bquery(db, table, entry_size, ranges),
        DBInstruction::Save(file) => save_to_file(db, &file),
    }
}
