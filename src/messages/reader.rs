/// A non-panicking cursor over a borrowed byte slice.
///
/// All methods return `Err(InvalidData)` on bounds violations rather than
/// panicking, making them safe to call with attacker-controlled lengths.
use std::io::{Error, ErrorKind};

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Bytes still unread.
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn need(&self, n: usize) -> Result<(), Error> {
        if self.remaining() < n {
            Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected end of packet",
            ))
        } else {
            Ok(())
        }
    }

    /// Read a single byte.
    pub fn read_u8(&mut self) -> Result<u8, Error> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read a big-endian u16.
    pub fn read_u16(&mut self) -> Result<u16, Error> {
        self.need(2)?;
        let hi = self.buf[self.pos] as u16;
        let lo = self.buf[self.pos + 1] as u16;
        self.pos += 2;
        Ok((hi << 8) | lo)
    }

    /// Read exactly `n` bytes.
    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], Error> {
        self.need(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Read `len` raw bytes and decode as UTF-8, returning `Err` (not panic)
    /// on invalid sequences.
    pub fn read_mqtt_string(&mut self, len: usize) -> Result<String, Error> {
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8 in string field"))
    }

    /// Read a u16 length-prefix then the UTF-8 string it describes.
    pub fn read_mqtt_string_prefixed(&mut self) -> Result<String, Error> {
        let len = self.read_u16()? as usize;
        self.read_mqtt_string(len)
    }

    /// Read an MQTT variable-length integer (1–4 bytes).
    /// Returns `(value, bytes_consumed)`.
    pub fn read_varint(&mut self) -> Result<(usize, usize), Error> {
        let start = self.pos;
        let mut lensize: usize = 1;

        self.need(1)?;
        let mut len: usize = (self.buf[self.pos] as usize) & 0x7F;

        while (self.buf[self.pos] & 0x80) == 0x80 {
            self.pos += 1;
            self.need(1)?;
            len |= ((self.buf[self.pos] as usize) & 0x7F) << (7 * lensize);
            lensize += 1;
            if lensize > 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "variable-length integer too long",
                ));
            }
        }
        self.pos += 1;
        Ok((len, self.pos - start))
    }

    /// Skip exactly `n` bytes.
    pub fn skip(&mut self, n: usize) -> Result<(), Error> {
        self.need(n)?;
        self.pos += n;
        Ok(())
    }

    /// Assert that all bytes have been consumed.  Use as the final
    /// "message length incorrect" check in each parser.
    pub fn expect_end(&self, context: &'static str) -> Result<(), Error> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::InvalidData, context))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_u8_basic() {
        let mut r = Reader::new(&[0xAB]);
        assert_eq!(r.read_u8().unwrap(), 0xAB);
        assert!(r.is_empty());
    }

    #[test]
    fn read_u8_empty_errors() {
        let mut r = Reader::new(&[]);
        assert!(r.read_u8().is_err());
    }

    #[test]
    fn read_u16_basic() {
        let mut r = Reader::new(&[0x01, 0x02]);
        assert_eq!(r.read_u16().unwrap(), 0x0102);
    }

    #[test]
    fn read_u16_short_errors() {
        let mut r = Reader::new(&[0x01]);
        assert!(r.read_u16().is_err());
    }

    #[test]
    fn read_bytes_basic() {
        let mut r = Reader::new(&[1, 2, 3, 4]);
        assert_eq!(r.read_bytes(2).unwrap(), &[1, 2]);
        assert_eq!(r.read_bytes(2).unwrap(), &[3, 4]);
    }

    #[test]
    fn read_bytes_overflow_errors() {
        let mut r = Reader::new(&[1, 2]);
        assert!(r.read_bytes(3).is_err());
    }

    #[test]
    fn read_mqtt_string_invalid_utf8() {
        // 0xFF is not valid UTF-8
        let mut r = Reader::new(&[0xFF]);
        assert!(r.read_mqtt_string(1).is_err());
    }

    #[test]
    fn read_varint_one_byte() {
        let mut r = Reader::new(&[0x7F]);
        assert_eq!(r.read_varint().unwrap(), (127, 1));
    }

    #[test]
    fn read_varint_two_bytes() {
        let mut r = Reader::new(&[0x80, 0x01]);
        assert_eq!(r.read_varint().unwrap(), (128, 2));
    }

    #[test]
    fn read_varint_truncated_errors() {
        // continuation bit set but no next byte
        let mut r = Reader::new(&[0x80]);
        assert!(r.read_varint().is_err());
    }

    #[test]
    fn read_varint_too_long_errors() {
        let mut r = Reader::new(&[0x80, 0x80, 0x80, 0x80, 0x00]);
        assert!(r.read_varint().is_err());
    }

    #[test]
    fn expect_end_passes_when_empty() {
        let mut r = Reader::new(&[0xAB]);
        r.read_u8().unwrap();
        assert!(r.expect_end("test").is_ok());
    }

    #[test]
    fn expect_end_fails_when_data_remains() {
        let r = Reader::new(&[0xAB, 0xCD]);
        assert!(r.expect_end("test").is_err());
    }
}
