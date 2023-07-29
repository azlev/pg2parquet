// https://www.postgresql.org/docs/current/protocol-replication.html

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

// pub struct XLogDataMessage {}

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
