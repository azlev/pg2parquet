// https://www.postgresql.org/docs/current/protocol-replication.html

use std::fmt;

use libpq::connection::PqBytes;

use crate::postgres::pglsn::Lsn;
use crate::postgres::pgtime::Pgtime;

pub const XLOG_DATA_ID: u8 = b'w';
pub const PRIMARY_KEEPALIVE_ID: u8 = b'k';
pub const STANDBY_STATUS_UPDATE_ID: u8 = b'r';
// pub const HOST_STANDBY_FEEDBACK_MESSAGE_ID: u8 = b'h';
// pub const NEW_ARCHIVE_ID: u8 = b'n';
// pub const MANIFEST_ID: u8 = b'm';
// pub const ARCHIVE_OR_MANIFEST_DATA_ID: u8 = b'd';
// pub const PROGRESS_REPORT_ID: u8 = b'p';

const BEGIN_ID: u8 = b'B';
const MESSAGE_ID: u8 = b'M';
const COMMIT_ID: u8 = b'C';
const ORIGIN_ID: u8 = b'O';
const RELATION_ID: u8 = b'R';
const TYPE_ID: u8 = b'Y';
const INSERT_ID: u8 = b'I';
const UPDATE_ID: u8 = b'U';
const DELETE_ID: u8 = b'D';
const TRUNCATE_ID: u8 = b'T';
const STREAM_START_ID: u8 = b'S';
const STREAM_STOP_ID: u8 = b'E';
const STREAM_COMMIT_ID: u8 = b'c';
const STREAM_ABORT_ID: u8 = b'A';
const BEGIN_PREPARE_ID: u8 = b'b';
const PREPARE_ID: u8 = b'P';
const COMMIT_PREPARED_ID: u8 = b'K';
const ROLLBACK_PREPARED_ID: u8 = b'r';
const STREAM_PREPARE_ID: u8 = b'p';

pub fn parse_keepalive(buffer: &PqBytes) -> (Lsn, Pgtime, bool) {
    let tmp: [u8; 8] = buffer[1..9].try_into().unwrap();
    let lsn = Lsn::lsn_from_be_bytes(tmp);
    let tmp2: [u8; 8] = buffer[9..17].try_into().unwrap();
    let pgtime = Pgtime::from_be_bytes(tmp2);
    let should_reply = match buffer[17] {
        0 => false,
        _ => true,
    };
    (lsn, pgtime, should_reply)
}

pub fn create_keepalive() -> [u8; 34] {
    let mut reply: [u8; 34] = [0; 34];
    reply[0] = STANDBY_STATUS_UPDATE_ID;
    let now = Pgtime::now();
    eprintln!("{}", now);
    let dd: [u8; 8] = now.0.to_be_bytes();
    for i in 0..8 {
        reply[25 + i] = dd[i]
    }
    reply[33] = 1;
    reply
}

pub struct ParseError {
    error_message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.error_message)
    }
}

impl fmt::Debug for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.error_message)
    }
}

// https://www.postgresql.org/docs/15/protocol-logicalrep-message-formats.html
pub fn parse_xlogdata(buffer: &PqBytes) -> Result<(Lsn, Lsn, Pgtime, u8), ParseError> {
    let mut pos = 1;
    let tmp: [u8; 8] = buffer[pos..(pos + 8)].try_into().unwrap();
    pos = pos + 8;
    let start: Lsn = Lsn::lsn_from_be_bytes(tmp);
    let tmp2: [u8; 8] = buffer[pos..(pos + 8)].try_into().unwrap();
    pos = pos + 8;
    let current: Lsn = Lsn::lsn_from_be_bytes(tmp2);
    let tmp3: [u8; 8] = buffer[pos..(pos + 8)].try_into().unwrap();
    pos = pos + 8;
    let time: Pgtime = Pgtime::from_be_bytes(tmp3);
    let id = buffer[pos];
    pos = pos + 1;
    match id {
        STREAM_START_ID | STREAM_STOP_ID | STREAM_COMMIT_ID | STREAM_ABORT_ID
        | BEGIN_PREPARE_ID | PREPARE_ID | COMMIT_PREPARED_ID | ROLLBACK_PREPARED_ID
        | STREAM_PREPARE_ID => eprintln!("DEBUG: not mesage {} skipped", id),
        BEGIN_ID => {
            let ret = parse_lr_begin_message(buffer, pos);
            pos = ret.0;
            let (lsn_final, transaction_start, xid) = (ret.1, ret.2, ret.3);
            eprintln!(
                "begin: LSN_FINAL: {}, time: {}, xid: {}",
                lsn_final, transaction_start, xid
            );
        }
        MESSAGE_ID => eprintln!("message"),
        COMMIT_ID => eprintln!("commit"),
        ORIGIN_ID => eprintln!("origin"),
        RELATION_ID => eprintln!("relation"),
        TYPE_ID => eprintln!("type"),
        INSERT_ID => {
            let ret = parse_lr_insert_message(buffer, pos);
            let tuple = &buffer[ret.0..buffer.len()];
            eprintln!("insert, xid: {}, oid: {}, new_tuple: {}, tuple: {:#?}",
                ret.1, ret.2, ret.3, tuple);
        },
        UPDATE_ID => eprintln!("update"),
        DELETE_ID => eprintln!("delete"),
        TRUNCATE_ID => eprintln!("truncate"),
        _ => {
            return Result::Err(ParseError {
                error_message: format!("Message type '{}' not implemented", id),
            })
        }
    };
    Result::Ok((start, current, time, buffer[25]))
}

fn parse_lr_begin_message(buffer: &PqBytes, mut position: usize) -> (usize, Lsn, Pgtime, i32) {
    let tmp: [u8; 8] = buffer[position..(position + 8)].try_into().unwrap();
    position = position + 8;
    let lsn_final: Lsn = Lsn::lsn_from_be_bytes(tmp);

    let tmp2: [u8; 8] = buffer[position..(position + 8)].try_into().unwrap();
    position = position + 8;
    let transaction_start: Pgtime = Pgtime::from_be_bytes(tmp2);

    let tmp3: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position = position + 4;
    let xid: i32 = i32::from_be_bytes(tmp3);

    (position, lsn_final, transaction_start, xid)
}

fn parse_lr_insert_message(buffer: &PqBytes, mut position: usize) -> (usize, i32, i32, u8) {
    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position = position + 4;
    let xid: i32 = i32::from_be_bytes(tmp);

    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position = position + 4;
    let oid: i32 = i32::from_be_bytes(tmp);

    let new_tuple: u8 = buffer[position];
    position = position + 1;

    (position, xid, oid, new_tuple)
}