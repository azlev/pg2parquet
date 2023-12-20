use chrono::{DateTime, Duration, NaiveDateTime, Utc};

// postgres/src/include/datatype/timestamp.h
// postgres/src/backend/utils/adt/timestamp.c

pub const UNIX_EPOCH_JDATE: i64 = 2440588; /* == date2j(1970, 1, 1) */
pub const POSTGRES_EPOCH_JDATE: i64 = 2451545; /* == date2j(2000, 1, 1) */
pub const SECS_PER_DAY: i64 = 86400;
pub const TIME_ADJUST: i64 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * 1_000_000 /* microseconds */;

pub struct Pgtime(pub i64);

impl Pgtime {
    pub fn now() -> Self {
        let utc: DateTime<Utc> = Utc::now();
        Pgtime(utc.timestamp_micros() - TIME_ADJUST)
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Pgtime {
        let i = i64::from_be_bytes(bytes);
        Pgtime(i)
    }
}

impl std::fmt::Display for Pgtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dt = NaiveDateTime::from_timestamp_micros(self.0).unwrap();
        let adjust = Duration::microseconds(TIME_ADJUST);
        dt = dt + adjust;
        write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S.%f"))
    }
}

#[cfg(test)]
mod tests {
    use crate::postgres::pgtime::Pgtime;

    #[test]
    fn pg_epoch() {
        let bytes: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        let pgtime = Pgtime::from_be_bytes(bytes);
        assert_eq!(format!("{pgtime}"), "2000-01-01 00:00:00.000000000");
    }
}
