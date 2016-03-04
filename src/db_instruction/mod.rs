extern crate tag_db;

use tag_db::Range;
use tag_db::DB;
use dberror::DBError;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct WriteAccess{
    pub rng: Range,
    pub val: Vec<u8>,
}

pub enum DBInstructionType {
    TGET = 1,
    TPUT = 2,
    TDEL = 3,
    TDELALL = 4,
    TLAST = 5,
}

pub fn instr_from_opcode<'a>(opcode : u64) -> Result<DBInstructionType,DBError<'a>>{
    if opcode >= DBInstructionType::TLAST as u64 {return Err(DBError::ProtocolError("instruction type unknown".into()))}
    match opcode {
        1 => Ok(DBInstructionType::TGET),
        2 => Ok(DBInstructionType::TPUT),
        3 => Ok(DBInstructionType::TDEL),
        4 => Ok(DBInstructionType::TDELALL),
        5 => Ok(DBInstructionType::TLAST),
        _ => unreachable!(),
    }
}

#[derive(Debug)]
pub enum DBInstruction {
    Get(String, Vec<Range>),
    Put(String, Vec<WriteAccess>),
    Del(String, Vec<Range>),
    DelAll(String, Vec<Range>),
    //BitmapPut(String, u32, u32, Vec<WriteAccess>) //table, rangesize, datasize, 
}

#[derive(Debug)]
pub enum DBResult {
    Done,
    Tags(HashMap<Range,Vec<u8>>),
}

fn get_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBResult, DBError<'a>> {
    let mut data = HashMap::new();
    let db_access = try!(db.read());
    for rng in rngs {
        if let Some(iter) = db_access.query(&table, rng) {
            for (r,v) in iter {
                data.insert(r.clone(), v.clone());
            }
        }
    }
    return Ok(DBResult::Tags(data))
}

fn put_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<WriteAccess>) -> Result<DBResult, DBError<'a>> {
    let mut db_access = try!(db.write());
    for w in rngs {
        db_access.insert(table.clone(), w.rng, w.val)
    }
    return Ok(DBResult::Done)
}

fn del_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBResult, DBError<'a>> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete(&table, rng)
    }
    return Ok(DBResult::Done)
}

fn delall_query<'a>(db: Arc<RwLock<Box<DB>>>, table: String, rngs: Vec<Range>) -> Result<DBResult, DBError<'a>> {
    let mut db_access = try!(db.write());
    for rng in rngs {
        db_access.delete_all(&table, rng)
    }
    return Ok(DBResult::Done)
}

pub fn execute_db_instruction<'a>(db: Arc<RwLock<Box<DB>>>, instr: DBInstruction) -> Result<DBResult, DBError<'a>>{
    match instr {
        DBInstruction::Get(table, ranges) => get_query(db, table, ranges),
        DBInstruction::Put(table, access) => put_query(db, table, access),
        DBInstruction::Del(table, ranges) => del_query(db, table, ranges),
        DBInstruction::DelAll(table, ranges) => delall_query(db, table, ranges),
    }
}
