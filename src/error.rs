extern crate rmp;
extern crate theban_db;
extern crate rmp_serialize as msgpack;

use std;
use theban_db::dberror::DBError;


quick_error! {
    #[derive(Debug)]
    pub enum NetworkEncodingError {
        Encodinge(err: msgpack::encode::Error) {
            from()
            description("network encoding error")
            display("Failed to encode for network: {}", err)
            cause(err)
        }
        Decodinge(err: msgpack::decode::Error) {
            from()
            description("network decoding error")
            display("Failed to decode from network: {}", err)
            cause(err)
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum DBServerError {
        NetworkEncoding(err: NetworkEncodingError){
            description("Failed to decode/encode data for networking")
            display("Failed to decode/encode data for networking: {}", err)
            from()
            cause(err)
        }
        DB(err: DBError){
            description("Error in DB")
            display("Error in DB: {}", err)
            from()
            cause(err)
        }
        SyncError{
            description("One thread paniced while holding a lock to the DB")
        }
    }
}

impl<'a,T> From<std::sync::PoisonError<T>> for DBServerError {
    fn from(_: std::sync::PoisonError<T>) -> DBServerError {
        DBServerError::SyncError
    }
}
