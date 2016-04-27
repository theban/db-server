extern crate unix_socket;
extern crate rmp;
extern crate memrange;

use std::io::Read;
use self::memrange::Range;
use dberror::DBError;
use db_instruction::{DBInstruction, WriteAccess, DBInstructionType, instr_from_opcode};

fn parse_string<'a>(stream: &mut Read) -> Result<String, DBError<'a>>{
    let strref = try!(parse_bindata(stream));
    let strval = try!(String::from_utf8(strref));
    return Ok(strval)
}

fn parse_bindata<'a>(mut stream: &mut Read) -> Result<Vec<u8>, DBError<'a>>{
    let len = try!(rmp::decode::read_str_len(&mut stream));
    let ulen = len as usize;
    let mut buf: Vec<u8>= vec![0;ulen]; 
    let n = try!(rmp::decode::read_full(&mut stream,&mut buf[0..ulen]));
    if n != ulen {
        return Err(DBError::ProtocolError("unexpected EOF".into()))
    }
    return Ok(buf)
}

fn parse_range<'a>(mut stream: &mut Read) -> Result<Range, DBError<'a>>{
    let tup_len = try!( rmp::decode::read_array_size(&mut stream) );
    if tup_len != 2 { 
        return Err(DBError::ProtocolError("range tuple should have size 2".into()));
    }
    let from = try!(rmp::decode::read_u64_loosely(&mut stream));
    let to = try!(rmp::decode::read_u64_loosely(&mut stream));
    return Ok(Range::new(from,to));
}

fn parse_range_args<'a>(mut stream: &mut Read) -> Result<Vec<Range>, DBError<'a>>{
    let args_len = try!( rmp::decode::read_array_size(&mut stream) );
    let mut res = Vec::with_capacity(args_len as usize);
    for _ in 0..args_len {
        res.push(try!(parse_range(stream)));
    }
    return Ok(res);
}

fn parse_write_arg<'a>(mut stream: &mut Read) -> Result<WriteAccess, DBError<'a>>{
    let tup_len = try!( rmp::decode::read_array_size(&mut stream) );
    if tup_len != 3 { return Err(DBError::ProtocolError("Write tuple should have size 3".into())) };
    let from = try!(rmp::decode::read_u64_loosely(&mut stream));
    let to = try!(rmp::decode::read_u64_loosely(&mut stream));
    let val = try!(parse_bindata(&mut stream));
    return Ok(WriteAccess{rng: Range::new(from,to),val: val });
}

fn parse_write_args<'a>(mut stream: &mut  Read) -> Result<Vec<WriteAccess>, DBError<'a>>{
    let args_len = try!( rmp::decode::read_array_size(&mut stream) );
    let mut res = Vec::with_capacity(args_len as usize );
    for _ in 0..args_len {
        res.push(try!(parse_write_arg(&mut stream)));
    }
    return Ok(res);
}

fn parse_table_instruction<'a>(mut stream: &mut Read,opcode: DBInstructionType, instr_len: u64) -> Result<DBInstruction, DBError<'a>>{
    if instr_len != 3 {return Err(DBError::ProtocolError("instruction tuple should have size 3".into()))}
    let table = try!( parse_string(&mut stream) );
    return match opcode {
        DBInstructionType::TOGET     =>  { let args = try!(parse_range_args(&mut stream)); return Ok(DBInstruction::OGet(table, args)); },
        DBInstructionType::TOPUT     =>  { let args = try!(parse_write_args(&mut stream)); return Ok(DBInstruction::OPut(table, args)); },
        DBInstructionType::TODEL     =>  { let args = try!(parse_range_args(&mut stream)); return Ok(DBInstruction::ODel(table, args)); },
        DBInstructionType::TODELALL  =>  { let args = try!(parse_range_args(&mut stream)); return Ok(DBInstruction::ODelAll(table, args)); },
        DBInstructionType::TBPUT  =>  { let args = try!(parse_write_args(&mut stream)); return Ok(DBInstruction::BPut(table,1, args)); },
        DBInstructionType::TBGET  =>  { let args = try!(parse_range_args(&mut stream)); return Ok(DBInstruction::BGet(table, args)); },
        DBInstructionType::TBDEL  =>  { let args = try!(parse_range_args(&mut stream)); return Ok(DBInstruction::BDel(table, 1, args)); },
        _ => unreachable!()
    }
}

fn parse_save_instruction<'a>(mut stream: &mut Read, instr_len: u64) -> Result<DBInstruction, DBError<'a>>{
    if instr_len != 2 {return Err(DBError::ProtocolError("save instruction tuple should have size 2".into()))}
    let filename = try!(parse_string(&mut stream));
    return Ok(DBInstruction::Save(filename));
}

pub fn parse_one_instruction<'a>(mut stream: &mut Read) -> Result<DBInstruction,DBError<'a>>{
    let tup_len = try!( rmp::decode::read_array_size(&mut stream) ) as u64 ;
    let opcode = try!( rmp::decode::read_u64_loosely(&mut stream));
    let instr = try!(instr_from_opcode(opcode));
    match instr {
        DBInstructionType::TSAVE => parse_save_instruction(&mut stream, tup_len),
        _ => parse_table_instruction(&mut stream, instr, tup_len),
    }
}
