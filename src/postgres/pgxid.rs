use libpq::connection::PqBytes;

pub struct Xid(pub u32);

impl std::fmt::Display for Xid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0 as u32)
    }
}

impl Xid {
    pub fn from_be_bytes(bytes: [u8; 4]) -> Xid {
        Xid(u32::from_be_bytes(bytes))
    }
    pub fn from_buffer(buffer: &PqBytes, mut position: usize) -> (usize, Xid) {
        let tmp: [u8; 4] = buffer[position..(position + 4)].try_into().unwrap();
        position += 4;
        (position, Xid::from_be_bytes(tmp))
    }
}
