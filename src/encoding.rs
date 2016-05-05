extern crate rmp_serialize as msgpack;
extern crate rustc_serialize;

use std::io::Write;
use std::io::Read;

use db_instruction::DBAnswer;
use db_instruction::DBInstruction;
use dberror::NetworkEncodingError;

use self::rustc_serialize::{Decodable,Encodable};
use self::msgpack::{Decoder,Encoder};

pub type NetworkEncodingResult<T> = Result<T,NetworkEncodingError>;

pub fn encode_answer(a: DBAnswer, writer: &mut Write) -> NetworkEncodingResult<()>{
    try!(a.encode(&mut Encoder::new(writer)));
    return Ok(())
}

pub fn decode_answer(reader: &mut Read) -> NetworkEncodingResult<DBAnswer>{
    let answer = try!(DBAnswer::decode(&mut Decoder::new(reader)));
    return Ok(answer);
}

pub fn encode_instruction(i: DBInstruction, writer: &mut Write) -> NetworkEncodingResult<()>{
    try!(i.encode(&mut Encoder::new(writer)));
    return Ok(())
}

pub fn encode_instruction_to_vec(i: DBInstruction) -> NetworkEncodingResult<Vec<u8>>{
    let mut buf = vec![];
    try!(encode_instruction(i, &mut buf));
    return Ok(buf);
}

pub fn decode_instruction(reader: &mut Read) -> NetworkEncodingResult<DBInstruction>{
    let instr = try!(DBInstruction::decode(&mut Decoder::new(reader)));
    return Ok(instr);
}
