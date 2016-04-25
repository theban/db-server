extern crate unix_socket;
extern crate tag_db_server;

//mod tag_db_server;

use std::fs;
use unix_socket::{UnixStream, UnixListener};

fn main(){
    let _ = fs::remove_file("./socket");
    let listener = UnixListener::bind("./socket").unwrap();
    tag_db_server::run_database(listener);
}
