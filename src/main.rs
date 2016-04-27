extern crate unix_socket;
extern crate theban_db_server;
extern crate theban_db;
extern crate argparse;

use argparse::{ArgumentParser, Store, StoreOption};

use std::fs;
use unix_socket::{UnixListener};
use theban_db::DB;

fn main(){
    let mut filename = None;
    let mut sockname = "./socket".to_string();
    {  // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description("Run a TagDB Server");
        ap.refer(&mut filename)
            .add_argument("db_file", StoreOption,
            "filename of db to load");
        ap.refer(&mut sockname)
            .add_option(&["-s","--socket"], Store,
            "path to unixsocket for communication");
        ap.parse_args_or_exit();
    }
    let db = match filename {
        None => DB::new(),
        Some(ref file) => DB::new_from_file(file).unwrap(),
    };
    let _ = fs::remove_file(&sockname);
    let listener = UnixListener::bind(&sockname).unwrap();
    theban_db_server::run_database(db,listener);
}
