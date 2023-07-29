pub struct Lsn(pub u64);

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0x00000000ffffffff)
    }
}

impl Lsn {
    pub fn lsn_from_be_bytes(buffer: [u8; 8]) -> Lsn {
        Lsn(u64::from_be_bytes(buffer))
    }
}
