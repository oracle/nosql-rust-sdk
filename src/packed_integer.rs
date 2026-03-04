//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use std::result::Result;

use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;

// This file implements reading and writing packed integer values.
// This is ported from JE's PackedInteger class.

// write_packed_i32 writes an i32 value as a packed sorted integer onto
// the end of the supplied u8 vector.
pub fn write_packed_i32(v: &mut Vec<u8>, val: i32) {
    // Values in the inclusive range [-119,120] are stored in a single
    // byte. For values outside that range, the first byte stores the
    // number of additional bytes. The additional bytes store
    // (value + 119 for negative and value - 121 for positive) as an
    // unsigned big endian integer.
    let mut i = val;
    if i < -119 {
        // If the value < -119, then first adjust the value by adding 119.
        // Then the adjusted value is stored as an unsigned big endian integer.
        i += 119;

        // write a dummy length into the first byte; this will be replaced
        // after we get the full size
        let offset = v.len();
        v.push(0);

        // Store the adjusted value as an unsigned big endian integer.
        // For a negative integer, from left to right, the first significant
        // byte is the byte which is not equal to 0xFF. Also please note that,
        // because the adjusted value is stored in big endian integer, we
        // extract the significant byte from left to right.
        //
        // In the left to right order, if the first byte of the adjusted value
        // is a significant byte, it will be stored in the 2nd byte of the buf.
        // Then we will look at the 2nd byte of the adjusted value to see if
        // this byte is the significant byte, if yes, this byte will be stored
        // in the 3rd byte of the buf, and the like.
        let ui: u32 = i as u32;
        if (ui | 0x00FFFFFF) != 0xFFFFFFFF {
            v.push((ui >> 24) as u8);
        }
        if (ui | 0x0000FFFF) != 0xFFFFFFFF {
            v.push((ui >> 16) as u8);
        }
        if (ui | 0x000000FF) != 0xFFFFFFFF {
            v.push((ui >> 8) as u8);
        }
        v.push(i as u8);

        // len is the length of the value part stored in buf. Because the
        // first byte of buf is used to store the length, we need to subtract
        // one.
        let len = v.len() - offset - 1;

        // The first byte stores the number of additional bytes. Here we store
        // the result of 0x08 - valueLen, rather than directly store valueLen.
        // The reason is to implement natural sort order for byte-by-byte
        // comparison.
        v[offset] = (0x8 - len) as u8;
        return;
    }

    if i > 120 {
        // If the value > 120, then first adjust the value by subtracting 121.
        // Then the adjusted value is stored as an unsigned big endian integer.
        i -= 121;

        // write a dummy length into the first byte; this will be replaced
        // after we get the full size
        let offset = v.len();
        v.push(0);

        // Store the adjusted value as an unsigned big endian integer.
        // For a positive integer, from left to right, the first significant
        // byte is the byte which is not equal to 0x00.
        //
        // In the left to right order, if the first byte of the adjusted value
        // is a significant byte, it will be stored in the 2nd byte of the buf.
        // Then we will look at the 2nd byte of the adjusted value to see if
        // this byte is the significant byte, if yes, this byte will be stored
        // in the 3rd byte of the buf, and the like.
        if (i & 0x7F000000) != 0 {
            v.push((i >> 24) as u8);
        }
        if (i & 0x7FFF0000) != 0 {
            v.push((i >> 16) as u8);
        }
        if (i & 0x7FFFFF00) != 0 {
            v.push((i >> 8) as u8);
        }
        v.push(i as u8);

        // len is the length of the value part stored in buf. Because the
        // first byte of buf is used to store the length, we need to subtract
        // one.
        let len = v.len() - offset - 1;

        // The first byte stores the number of additional bytes. Here we store
        // the result of 0xF7 + valueLen, rather than directly store valueLen.
        // The reason is to implement natural sort order for byte-by-byte
        // comparison.
        v[offset] = (0xF7 + len) as u8;

        return;
    }

    v.push((i + 127) as u8);
}

// write_packed_i64 writes an i64 value as a packed sorted integer onto
// the end of the supplied u8 vector.
pub fn write_packed_i64(v: &mut Vec<u8>, val: i64) {
    // Values in the inclusive range [-119,120] are stored in a single
    // byte. For values outside that range, the first byte stores the
    // number of additional bytes. The additional bytes store
    // (value + 119 for negative and value - 121 for positive) as an
    // unsigned big endian integer.
    let mut i = val;
    if i < -119 {
        // If the value < -119, then first adjust the value by adding 119.
        // Then the adjusted value is stored as an unsigned big endian integer.
        i += 119;

        // write a dummy length into the first byte; this will be replaced
        // after we get the full size
        let offset = v.len();
        v.push(0);

        // Store the adjusted value as an unsigned big endian integer.
        // For a negative integer, from left to right, the first significant
        // byte is the byte which is not equal to 0xFF. Also please note that,
        // because the adjusted value is stored in big endian integer, we
        // extract the significant byte from left to right.
        //
        // In the left to right order, if the first byte of the adjusted value
        // is a significant byte, it will be stored in the 2nd byte of the buf.
        // Then we will look at the 2nd byte of the adjusted value to see if
        // this byte is the significant byte, if yes, this byte will be stored
        // in the 3rd byte of the buf, and the like.
        let ui: u64 = i as u64;
        if (ui | 0x00FFFFFFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 56) as u8);
        }
        if (ui | 0x0000FFFFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 48) as u8);
        }
        if (ui | 0x000000FFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 40) as u8);
        }
        if (ui | 0x00000000FFFFFFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 32) as u8);
        }
        if (ui | 0x0000000000FFFFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 24) as u8);
        }
        if (ui | 0x000000000000FFFF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 16) as u8);
        }
        if (ui | 0x00000000000000FF) != 0xFFFFFFFFFFFFFFFF {
            v.push((ui >> 8) as u8);
        }
        v.push(i as u8);

        // len is the length of the value part stored in buf. Because the
        // first byte of buf is used to store the length, we need to subtract
        // one.
        let len = v.len() - offset - 1;

        // The first byte stores the number of additional bytes. Here we store
        // the result of 0x08 - valueLen, rather than directly store valueLen.
        // The reason is to implement natural sort order for byte-by-byte
        // comparison.
        v[offset] = (0x8 - len) as u8;
        return;
    }

    if i > 120 {
        // If the value > 120, then first adjust the value by subtracting 121.
        // Then the adjusted value is stored as an unsigned big endian integer.
        i -= 121;

        // write a dummy length into the first byte; this will be replaced
        // after we get the full size
        let offset = v.len();
        v.push(0);

        // Store the adjusted value as an unsigned big endian integer.
        // For a positive integer, from left to right, the first significant
        // byte is the byte which is not equal to 0x00.
        //
        // In the left to right order, if the first byte of the adjusted value
        // is a significant byte, it will be stored in the 2nd byte of the buf.
        // Then we will look at the 2nd byte of the adjusted value to see if
        // this byte is the significant byte, if yes, this byte will be stored
        // in the 3rd byte of the buf, and the like.
        if (i & 0x7F00000000000000) != 0 {
            v.push((i >> 56) as u8);
        }
        if (i & 0x7FFF000000000000) != 0 {
            v.push((i >> 48) as u8);
        }
        if (i & 0x7FFFFF0000000000) != 0 {
            v.push((i >> 40) as u8);
        }
        if (i & 0x7FFFFFFF00000000) != 0 {
            v.push((i >> 32) as u8);
        }
        if (i & 0x7FFFFFFFFF000000) != 0 {
            v.push((i >> 24) as u8);
        }
        if (i & 0x7FFFFFFFFFFF0000) != 0 {
            v.push((i >> 16) as u8);
        }
        if (i & 0x7FFFFFFFFFFFFF00) != 0 {
            v.push((i >> 8) as u8);
        }
        v.push(i as u8);

        // len is the length of the value part stored in buf. Because the
        // first byte of buf is used to store the length, we need to subtract
        // one.
        let len = v.len() - offset - 1;

        // The first byte stores the number of additional bytes. Here we store
        // the result of 0xF7 + valueLen, rather than directly store valueLen.
        // The reason is to implement natural sort order for byte-by-byte
        // comparison.
        v[offset] = (0xF7 + len) as u8;

        return;
    }

    v.push((i + 127) as u8);
}

// check that the offset will not run off the end of the vector, then
// increment it.
fn increment_and_check(offset: &mut usize, len: usize) -> Result<(), NoSQLError> {
    if *offset >= len {
        return Err(NoSQLError::new(
            BadProtocolMessage,
            "attempt to read past end of buffer",
        ));
    }
    *offset += 1;
    Ok(())
}

// read a packed i32 from the given vector at the given offset.
pub fn read_packed_i32(buf: &mut Vec<u8>, offset: &mut usize) -> Result<i32, NoSQLError> {
    if buf.len() <= *offset {
        return Err(NoSQLError::new(
            BadProtocolMessage,
            "invalid packed_i32 in buffer",
        ));
    }
    // The first byte stores the length of the value part.
    let mut len: u8 = buf[*offset];
    increment_and_check(offset, buf.len())?;

    let mut is_negative: bool = false;

    // Adjust the len to the real length of the value part.
    if len < 0x08 {
        len = 0x08 - len;
        is_negative = true;
    } else if len > 0xF7 {
        len = len - 0xF7;
    } else {
        return Ok((len as i32) - 127);
    }

    // The following bytes on the buf store the value as a big endian integer.
    // We extract the significant bytes from the buf and put them into the
    // value in big endian order.
    let mut value: i32 = 0;
    if is_negative {
        value = -1; // 0xFFFFFFFF
    }

    while len > 1 {
        value = (value << 8) | (buf[*offset] as i32);
        increment_and_check(offset, buf.len())?;
        len -= 1;
    }
    value = (value << 8) | (buf[*offset] as i32);
    increment_and_check(offset, buf.len())?;

    // After get the adjusted value, we have to adjust it back to the
    // original value.
    if is_negative {
        value -= 119;
    } else {
        value += 121;
    }
    Ok(value)
}

// read a packed i64 from the given vector at the given offset.
pub fn read_packed_i64(buf: &mut Vec<u8>, offset: &mut usize) -> Result<i64, NoSQLError> {
    if buf.len() <= *offset {
        return Err(NoSQLError::new(
            BadProtocolMessage,
            "invalid packed_i64 in buffer",
        ));
    }
    // The first byte stores the length of the value part.
    let mut len: u8 = buf[*offset];
    increment_and_check(offset, buf.len())?;

    let mut is_negative: bool = false;

    // Adjust the len to the real length of the value part.
    if len < 0x08 {
        len = 0x08 - len;
        is_negative = true;
    } else if len > 0xF7 {
        len = len - 0xF7;
    } else {
        return Ok((len as i64) - 127);
    }

    // The following bytes on the buf store the value as a big endian integer.
    // We extract the significant bytes from the buf and put them into the
    // value in big endian order.
    let mut value: i64 = 0;
    if is_negative {
        value = -1; // 0xFFFFFFFFFFFFFFFF
    }

    while len > 1 {
        value = (value << 8) | (buf[*offset] as i64);
        increment_and_check(offset, buf.len())?;
        len -= 1;
    }
    value = (value << 8) | (buf[*offset] as i64);
    increment_and_check(offset, buf.len())?;

    // After get the adjusted value, we have to adjust it back to the
    // original value.
    if is_negative {
        value -= 119;
    } else {
        value += 121;
    }
    Ok(value)
}
