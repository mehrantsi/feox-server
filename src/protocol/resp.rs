use bytes::{Bytes, BytesMut};
use memchr::memchr2;
use std::str;

/// RESP (REdis Serialization Protocol) parser
pub struct RespParser {
    buffer: BytesMut,
    position: usize,
}

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(16 * 1024),
            position: 0,
        }
    }

    /// Feed data into the parser
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Parse next complete RESP value
    pub fn parse_next(&mut self) -> Result<Option<RespValue>, String> {
        if self.position >= self.buffer.len() {
            return Ok(None);
        }

        let remaining = &self.buffer[self.position..];

        match self.parse_value(remaining) {
            Ok(Some((value, consumed))) => {
                self.position += consumed;

                // Compact buffer if needed
                if self.position > self.buffer.len() / 2 {
                    let _ = self.buffer.split_to(self.position);
                    self.position = 0;
                }

                Ok(Some(value))
            }
            Ok(None) => Ok(None), // Need more data
            Err(e) => Err(e),
        }
    }

    /// Parse a RESP value from buffer
    fn parse_value(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        if buf.is_empty() {
            return Ok(None);
        }

        match buf[0] {
            b'+' => self.parse_simple_string(buf),
            b'-' => self.parse_error(buf),
            b':' => self.parse_integer(buf),
            b'$' => self.parse_bulk_string(buf),
            b'*' => self.parse_array(buf),
            _ => Err(format!("Invalid RESP type: {}", buf[0] as char)),
        }
    }

    /// Parse simple string: +OK\r\n
    fn parse_simple_string(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        if let Some(end) = find_crlf(buf) {
            let data = Bytes::copy_from_slice(&buf[1..end]);
            Ok(Some((RespValue::SimpleString(data), end + 2)))
        } else {
            Ok(None)
        }
    }

    /// Parse error: -ERR message\r\n
    fn parse_error(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        if let Some(end) = find_crlf(buf) {
            let msg = str::from_utf8(&buf[1..end])
                .map_err(|_| "Invalid UTF-8 in error")?
                .to_string();
            Ok(Some((RespValue::Error(msg), end + 2)))
        } else {
            Ok(None)
        }
    }

    /// Parse integer: :123\r\n
    fn parse_integer(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        if let Some(end) = find_crlf(buf) {
            let num_str = str::from_utf8(&buf[1..end]).map_err(|_| "Invalid UTF-8 in integer")?;
            let num = num_str
                .parse::<i64>()
                .map_err(|_| format!("Invalid integer: {}", num_str))?;
            Ok(Some((RespValue::Integer(num), end + 2)))
        } else {
            Ok(None)
        }
    }

    /// Parse bulk string: $6\r\nfoobar\r\n or $-1\r\n (null)
    fn parse_bulk_string(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        // Find length line
        let len_end = match find_crlf(buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        // Parse length
        let len_str =
            str::from_utf8(&buf[1..len_end]).map_err(|_| "Invalid UTF-8 in bulk string length")?;
        let len = len_str
            .parse::<i64>()
            .map_err(|_| format!("Invalid bulk string length: {}", len_str))?;

        if len < 0 {
            // Null bulk string
            return Ok(Some((RespValue::BulkString(None), len_end + 2)));
        }

        let len = len as usize;
        let data_start = len_end + 2;
        let data_end = data_start + len;

        // Check if we have enough data
        if buf.len() < data_end + 2 {
            return Ok(None);
        }

        // Verify CRLF after data
        if buf[data_end] != b'\r' || buf[data_end + 1] != b'\n' {
            return Err("Missing CRLF after bulk string".to_string());
        }

        // Zero-copy: create Bytes view into buffer
        let data = Bytes::copy_from_slice(&buf[data_start..data_end]);
        Ok(Some((RespValue::BulkString(Some(data)), data_end + 2)))
    }

    /// Parse array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    fn parse_array(&self, buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
        // Find length line
        let len_end = match find_crlf(buf) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        // Parse length
        let len_str =
            str::from_utf8(&buf[1..len_end]).map_err(|_| "Invalid UTF-8 in array length")?;
        let len = len_str
            .parse::<i64>()
            .map_err(|_| format!("Invalid array length: {}", len_str))?;

        if len < 0 {
            // Null array
            return Ok(Some((RespValue::Array(None), len_end + 2)));
        }

        let len = len as usize;
        let mut elements = Vec::with_capacity(len);
        let mut pos = len_end + 2;

        // Parse array elements
        for _ in 0..len {
            match self.parse_value(&buf[pos..])? {
                Some((value, consumed)) => {
                    elements.push(value);
                    pos += consumed;
                }
                None => return Ok(None), // Need more data
            }
        }

        Ok(Some((RespValue::Array(Some(elements)), pos)))
    }
}

/// Find CRLF in buffer
#[inline]
fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut pos = 0;
    while pos < buf.len() - 1 {
        if let Some(cr_pos) = memchr2(b'\r', b'\n', &buf[pos..]) {
            let cr_pos = pos + cr_pos;
            if cr_pos < buf.len() - 1 && buf[cr_pos] == b'\r' && buf[cr_pos + 1] == b'\n' {
                return Some(cr_pos);
            }
            pos = cr_pos + 1;
        } else {
            break;
        }
    }
    None
}

/// Format response as RESP with pre-allocated buffer
pub fn format_resp_response(value: &RespValue) -> Vec<u8> {
    let mut result = Vec::with_capacity(estimate_resp_size(value));
    write_resp_value(&mut result, value);
    result
}

/// Write RESP value directly to buffer (zero-copy when possible)
pub fn write_resp_value(buf: &mut Vec<u8>, value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            buf.push(b'-');
            buf.extend_from_slice(e.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            buf.push(b':');
            let mut num_buf = itoa::Buffer::new();
            buf.extend_from_slice(num_buf.format(*n).as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(Some(s)) => {
            buf.push(b'$');
            let mut num_buf = itoa::Buffer::new();
            buf.extend_from_slice(num_buf.format(s.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(s);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Array(Some(arr)) => {
            buf.push(b'*');
            let mut num_buf = itoa::Buffer::new();
            buf.extend_from_slice(num_buf.format(arr.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in arr {
                write_resp_value(buf, item);
            }
        }
        RespValue::Array(None) => {
            buf.extend_from_slice(b"*-1\r\n");
        }
    }
}

/// Estimate the size needed for a RESP value (for pre-allocation)
#[inline]
fn estimate_resp_size(value: &RespValue) -> usize {
    match value {
        RespValue::SimpleString(s) => s.len() + 3,
        RespValue::Error(e) => e.len() + 3,
        RespValue::Integer(_) => 24, // :number\r\n
        RespValue::BulkString(Some(s)) => s.len() + 20,
        RespValue::BulkString(None) => 5,
        RespValue::Array(Some(arr)) => {
            let mut size = 10; // array header
            for item in arr {
                size += estimate_resp_size(item);
            }
            size
        }
        RespValue::Array(None) => 5,
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}
