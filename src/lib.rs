extern crate unix_socket;
extern crate byteorder;
extern crate tag_db;

mod dberror;
mod db_instruction;
mod mp_parser;
mod mp_serialize;

use std::thread;
use std::sync::RwLock;
use std::sync::Arc;
use std::fs;
use unix_socket::{UnixStream, UnixListener};
use dberror::DBError;
use mp_parser::parse_one_instruction;
use tag_db::DB;

fn client_loop<'a>(db: Arc<RwLock<Box<DB>>>, mut stream: UnixStream) -> Result<(), DBError<'a>>{
    loop {
        let instr = try!(parse_one_instruction(&mut stream));
        //println!("Client said: {:?}", &instr);
        let resp = try!(db_instruction::execute_db_instruction(db.clone(), instr));
        //println!("execution result {:?}", resp);
        try!(mp_serialize::print_serialize_result(resp, &mut stream));
    }
}

fn handle_client(stream: UnixStream, dblock: Arc<RwLock<Box<DB>>>){
    let res = client_loop(dblock, stream);
    //println!("Connection Terminated: {:?}", res);
}

pub fn run_database(listener: UnixListener){
    let dblock = Arc::new(RwLock::new(Box::new(DB::new())));

    // accept connections and process them, spawning a new thread for each one
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let arc = dblock.clone();
                thread::spawn(|| handle_client(stream, arc));
            }
            Err(err) => {
                /* connection failed */
                //println!("Connection failed due to {}",err);
                break;
            }
        }
    }

    // close the listener socket
    drop(listener);
}
