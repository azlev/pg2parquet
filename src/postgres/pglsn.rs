use libpq::connection::PqBytes;

pub struct Lsn(pub u64);

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0x00000000ffffffff)
    }
}

impl Lsn {
    pub fn from_be_bytes(bytes: [u8; 8]) -> Lsn {
        Lsn(u64::from_be_bytes(bytes))
    }
    pub fn from_buffer(buffer: &PqBytes, mut position: usize) -> (usize, Lsn) {
        let tmp: [u8; 8] = buffer[position..(position + 8)].try_into().unwrap();
        position += 8;
        (position, Lsn::from_be_bytes(tmp))
    }
}

#[cfg(test)]
mod tests {
    use crate::postgres::pglsn::Lsn;

    #[test]
    fn lsn_zero() {
        let bytes: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        let lsn = Lsn::from_be_bytes(bytes);
        assert_eq!(format!("{lsn}"), "0/0");
    }
}
