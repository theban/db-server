extern crate unix_socket;
extern crate byteorder;
extern crate theban_db;
extern crate rustc_serialize;
#[macro_use] extern crate quick_error;

mod dberror;
mod db_instruction;
mod encoding;

use std::thread;
use std::sync::RwLock;
use std::sync::Arc;
use unix_socket::{UnixStream, UnixListener};
use dberror::DBServerError;
use theban_db::DB;

fn client_loop<'a>(db: Arc<RwLock<Box<DB>>>, mut stream: UnixStream) -> Result<(), DBServerError>{
    loop {
        let instr = try!(encoding::decode_instruction(&mut stream));
        //println!("Client said: {:?}", &instr);
        let resp = try!(db_instruction::execute_db_instruction(db.clone(), instr));
        //println!("execution result {:?}", resp);
        try!(encoding::encode_answer(resp, &mut stream));
    }
}

fn handle_client(stream: UnixStream, dblock: Arc<RwLock<Box<DB>>>){
    let _ = client_loop(dblock, stream);
    //let res = client_loop(dblock, stream);
    //println!("Connection Terminated: {:?}", res);
}

pub fn run_database(db: DB, listener: UnixListener){
    let dblock = Arc::new(RwLock::new(Box::new(db)));

    // accept connections and process them, spawning a new thread for each one
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let arc = dblock.clone();
                thread::spawn(|| handle_client(stream, arc));
            }
            Err(err) => {
                /* connection failed */
                println!("Connection failed due to {}",err);
                break;
            }
        }
    }

    // close the listener socket
    drop(listener);
}
