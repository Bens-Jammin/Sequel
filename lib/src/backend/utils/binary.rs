// this is used to help with binary information conversion


pub fn u16_to_u8(data: u16) -> (u8, u8) {
    let byte_1: u8 = (data >> 8) as u8;
    let byte_2: u8 = (data & 0xFF) as u8;
    (byte_1, byte_2)
}

pub fn u8_to_u16(byte_1: u8, byte_2: u8) -> u16 {
    ((byte_1 as u16) << 8) + byte_2 as u16
}