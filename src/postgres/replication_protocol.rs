// https://www.postgresql.org/docs/current/protocol-replication.html

use std::fmt;

use libpq::connection::PqBytes;
use libpq::Oid;

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

// postgres/src/include/replication/logicalproto.h
const LOGICAL_REP_MSG_BEGIN: u8 = b'B';
const LOGICAL_REP_MSG_COMMIT: u8 = b'C';
const LOGICAL_REP_MSG_ORIGIN: u8 = b'O';
const LOGICAL_REP_MSG_INSERT: u8 = b'I';
const LOGICAL_REP_MSG_UPDATE: u8 = b'U';
const LOGICAL_REP_MSG_DELETE: u8 = b'D';
const LOGICAL_REP_MSG_TRUNCATE: u8 = b'T';
const LOGICAL_REP_MSG_RELATION: u8 = b'R';
const LOGICAL_REP_MSG_TYPE: u8 = b'Y';
const LOGICAL_REP_MSG_MESSAGE: u8 = b'M';
const LOGICAL_REP_MSG_BEGIN_PREPARE: u8 = b'b';
const LOGICAL_REP_MSG_PREPARE: u8 = b'P';
const LOGICAL_REP_MSG_COMMIT_PREPARED: u8 = b'K';
const LOGICAL_REP_MSG_ROLLBACK_PREPARED: u8 = b'r';
const LOGICAL_REP_MSG_STREAM_START: u8 = b'S';
const LOGICAL_REP_MSG_STREAM_STOP: u8 = b'E';
const LOGICAL_REP_MSG_STREAM_COMMIT: u8 = b'c';
const LOGICAL_REP_MSG_STREAM_ABORT: u8 = b'A';
const LOGICAL_REP_MSG_STREAM_PREPARE: u8 = b'p';

pub fn parse_keepalive(buffer: &PqBytes) -> (Lsn, Pgtime, bool) {
    let lsn: Lsn;
    (_, lsn) = Lsn::from_buffer(buffer, 1);
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
    eprintln!("{now}");
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

fn parse_string(buffer: &PqBytes, mut position: usize) -> (usize, String) {
    let mut name: String = String::new();
    while buffer[position] != b'\0' {
        name.push(buffer[position] as char);
        position += 1;
    }
    position += 1;
    (position, name)
}

// https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html
pub fn parse_xlogdata(buffer: &PqBytes) -> Result<(Lsn, Lsn, Pgtime, char), ParseError> {
    let mut pos = 1;

    let start: Lsn;
    (pos, start) = Lsn::from_buffer(buffer, pos);

    let current: Lsn;
    (pos, current) = Lsn::from_buffer(buffer, pos);

    let tmp: [u8; 8] = buffer[pos..(pos + 8)].try_into().unwrap();
    pos += 8;
    let time: Pgtime = Pgtime::from_be_bytes(tmp);
    let id = buffer[pos];
    pos += 1;
    let streaming = false;
    match id {
        LOGICAL_REP_MSG_STREAM_START
        | LOGICAL_REP_MSG_STREAM_STOP
        | LOGICAL_REP_MSG_STREAM_COMMIT
        | LOGICAL_REP_MSG_STREAM_ABORT
        | LOGICAL_REP_MSG_BEGIN_PREPARE
        | LOGICAL_REP_MSG_PREPARE
        | LOGICAL_REP_MSG_COMMIT_PREPARED
        | LOGICAL_REP_MSG_ROLLBACK_PREPARED
        | LOGICAL_REP_MSG_STREAM_PREPARE => eprintln!("DEBUG: not mesage {id} skipped"),
        LOGICAL_REP_MSG_BEGIN => {
            let ret = parse_lr_begin_message(buffer, pos);
            //pos = ret.0;
            let (lsn_final, transaction_start, xid) = (ret.1, ret.2, ret.3);
            eprintln!("begin: LSN_FINAL: {lsn_final}, time: {transaction_start}, xid: {xid}");
        }
        LOGICAL_REP_MSG_MESSAGE => eprintln!("message"),
        LOGICAL_REP_MSG_COMMIT => eprintln!("commit"),
        LOGICAL_REP_MSG_ORIGIN => eprintln!("origin"),
        LOGICAL_REP_MSG_RELATION => {
            _ = parse_lr_relation(buffer, pos, streaming);
        }
        LOGICAL_REP_MSG_TYPE => eprintln!("type"),
        LOGICAL_REP_MSG_INSERT => {
            let ret = parse_lr_dml_message(buffer, pos, streaming);
            eprintln!("insert, xid: {}, oid: {}, kind: {}", ret.1, ret.2, ret.3);
            pos = ret.0;
            (_, _) = parse_lr_tupledata(buffer, pos);
        }
        LOGICAL_REP_MSG_UPDATE => {
            let ret = parse_lr_dml_message(buffer, pos, streaming);
            eprintln!("update, xid: {}, oid: {}, kind: {}", ret.1, ret.2, ret.3);
            pos = ret.0;
            if ret.3 != 'N' {
                (pos, _) = parse_lr_tupledata(buffer, pos);
                let _kind: char = buffer[pos] as char;
                pos += 1;
            }
            (_, _) = parse_lr_tupledata(buffer, pos);
        }
        LOGICAL_REP_MSG_DELETE => {
            let ret = parse_lr_dml_message(buffer, pos, streaming);
            eprintln!("delete, xid: {}, oid: {}, kind: {}", ret.1, ret.2, ret.3);
            pos = ret.0;
            (_, _) = parse_lr_tupledata(buffer, pos);
        }
        LOGICAL_REP_MSG_TRUNCATE => eprintln!("truncate"),
        _ => {
            return Result::Err(ParseError {
                error_message: format!("Message type '{id}' not implemented"),
            })
        }
    };
    Result::Ok((start, current, time, buffer[25] as char))
}

fn parse_lr_begin_message(buffer: &PqBytes, mut position: usize) -> (usize, Lsn, Pgtime, i32) {
    let lsn_final: Lsn;
    (position, lsn_final) = Lsn::from_buffer(buffer, position);

    let tmp2: [u8; 8] = buffer[position..(position + 8)].try_into().unwrap();
    position += 8;
    let transaction_start: Pgtime = Pgtime::from_be_bytes(tmp2);

    let tmp3: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position += 4;
    let xid: i32 = i32::from_be_bytes(tmp3);

    (position, lsn_final, transaction_start, xid)
}

fn parse_lr_relation(buffer: &PqBytes, mut position: usize, streaming: bool) -> usize {
    let mut _xid: i32 = 0;
    if streaming {
        let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
        position += 4;
        _xid = i32::from_be_bytes(tmp);
    }
    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position += 4;
    let oid: Oid = i32::from_be_bytes(tmp) as Oid;

    let namespace: String;
    (position, namespace) = parse_string(buffer, position);

    let relation: String;
    (position, relation) = parse_string(buffer, position);

    let replica_identity = buffer[position];
    position += 1;

    let tmp: [u8; 2] = buffer[position..(position + 2)].try_into().unwrap();
    position += 2;
    let columns: u16 = u16::from_be_bytes(tmp);

    for _i in 0..columns {
        position = parse_lr_relation_column(buffer, position)
    }

    eprintln!("relation: oid: {oid}, namespace: {namespace}, relation: {relation}, replica_identity: {replica_identity}, columns: {columns}");
    position
}

fn parse_lr_relation_column(buffer: &PqBytes, mut position: usize) -> usize {
    let flags = buffer[position];
    position += 1;

    let name: String;
    (position, name) = parse_string(buffer, position);

    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position += 4;
    let oid: Oid = i32::from_be_bytes(tmp) as Oid;

    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position += 4;
    let modifiers: i32 = i32::from_be_bytes(tmp);

    eprintln!("\tcolumn: {name}, flags: {flags}, oid: {oid}, mod: {modifiers}");
    position
}

fn parse_lr_dml_message(
    buffer: &PqBytes,
    mut position: usize,
    streaming: bool,
) -> (usize, i32, Oid, char) {
    let mut xid: i32 = 0;
    if streaming {
        let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
        position += 4;
        xid = i32::from_be_bytes(tmp);
    }

    let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
    position += 4;
    let oid: Oid = i32::from_be_bytes(tmp) as Oid;

    let kind: char = buffer[position] as char;
    position += 1;

    (position, xid, oid, kind)
}

fn parse_lr_tupledata(buffer: &PqBytes, mut position: usize) -> (usize, i16) {
    let tmp: [u8; 2] = buffer[position..(position + 2)].try_into().unwrap();
    position += 2;
    let ncolumns = i16::from_be_bytes(tmp);

    eprintln!("  columns: {ncolumns}");
    for _i in 0..ncolumns {
        (position, _) = parse_lr_tupledata_column(buffer, position);
    }
    (position, ncolumns)
}

fn parse_lr_tupledata_column(buffer: &PqBytes, mut position: usize) -> (usize, u8) {
    let kind = buffer[position];
    position += 1;

    match kind {
        b'n' | b'u' => eprintln!("\tcolumn kind: {}", kind as char),
        b't' => {
            let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
            position += 4;

            let length: usize = i32::from_be_bytes(tmp).try_into().unwrap();

            let t: String =
                String::from_utf8(buffer[position..(position + length)].to_vec()).unwrap();
            position += length;

            eprintln!("\tcolumn kind: {}, length: {length}, '{t}'", kind as char);
        }
        b'b' => {
            let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
            position += 4;

            let length: usize = i32::from_be_bytes(tmp).try_into().unwrap();
            position += length;

            eprintln!("\tcolumn kind: {}, length: {length}", kind as char);
        }
        _ => panic!("Unknown kind: {}", kind as char),
    }
    (position, kind)
}
