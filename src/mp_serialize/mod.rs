extern crate rmp;
extern crate memrange;

use std::io::Write;
use std::collections::HashMap;
use dberror::DBError;
use db_instruction::DBResult;
use self::memrange::Range;
use theban_db::Bitmap;

fn print_serialize_done<'a>(mut sink: &mut Write) -> Result<(),DBError<'a>> {
    try!(rmp::encode::write_array_len(&mut sink, 1));
    try!(rmp::encode::write_uint(&mut sink, 1));
    return Ok(())
}

fn print_serialize_tags<'a>(data: HashMap<Range,Vec<u8>>, mut sink: &mut Write) -> Result<(),DBError<'a>> {

    try!(rmp::encode::write_array_len(&mut sink, data.len() as u32 ));
    for (k,v) in data {
        try!(rmp::encode::write_array_len(&mut sink, 3));
        try!(rmp::encode::write_uint(&mut sink, k.min));
        try!(rmp::encode::write_uint(&mut sink, k.max));
        try!(rmp::encode::write_str_len(&mut sink, v.len() as u32));
        try!(sink.write_all(&v));
    }
    return Ok(())
}

fn print_serialize_bitmaps<'a>(data: HashMap<Range,Bitmap>, mut sink: &mut Write) -> Result<(),DBError<'a>> {

    try!(rmp::encode::write_array_len(&mut sink, data.len() as u32 ));
    for (k,v) in data {
        try!(rmp::encode::write_array_len(&mut sink, 4));
        try!(rmp::encode::write_uint(&mut sink, k.min));
        try!(rmp::encode::write_uint(&mut sink, k.max));
        try!(rmp::encode::write_uint(&mut sink, v.entry_size));
        try!(rmp::encode::write_str_len(&mut sink, v.data.len() as u32));
        try!( sink.write_all(&v.data) );
    }
    return Ok(())
}

pub fn print_serialize_result<'a>(res: DBResult, mut sink: &mut Write) -> Result<(),DBError<'a>> {
    match res {
        DBResult::Done => print_serialize_done(sink),
        DBResult::Tags(data) => print_serialize_tags(data, sink),
        DBResult::Bitmap(data) => print_serialize_bitmaps(data, sink),
    }
}
