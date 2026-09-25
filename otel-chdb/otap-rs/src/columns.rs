//! Minimal column buffers that become Arrow arrays without copying.
//!
//! Strings are kept as bytes and exported as Arrow `Binary`: OTLP strings are
//! not guaranteed to be UTF-8 (the hostile dataset has invalid UTF-8, which
//! the clickhouse exporter and parquetgo store unchanged), and an Arrow
//! `Utf8` array must be valid UTF-8. The Parquet schema handed to the writer
//! still annotates these columns as STRING (see `schema.rs`), so the file is
//! the same as parquetgo's.

use arrow::array::{ArrayRef, BinaryArray, ListArray, MapArray, PrimitiveArray, StructArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, FieldRef, Fields};
use std::sync::Arc;

/// A column of byte strings.
#[derive(Default)]
pub struct Bin {
    pub off: Vec<i32>,
    pub data: Vec<u8>,
}

impl Bin {
    pub fn clear(&mut self) {
        self.off.clear();
        self.off.push(0);
        self.data.clear();
    }
    #[inline]
    pub fn push(&mut self, b: &[u8]) {
        self.data.extend_from_slice(b);
        self.off.push(self.data.len() as i32);
    }
    /// Ends a value that was appended to `data` directly.
    #[inline]
    pub fn commit(&mut self) {
        self.off.push(self.data.len() as i32);
    }
    pub fn len(&self) -> usize {
        self.off.len().saturating_sub(1)
    }
    pub fn take(&mut self) -> ArrayRef {
        let off = std::mem::replace(&mut self.off, vec![0]);
        let data = std::mem::take(&mut self.data);
        // Offsets are built monotonically by push/commit.
        let off = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(off)) };
        Arc::new(BinaryArray::new(off, Buffer::from_vec(data), None))
    }
}

/// `n` copies of one value.
pub fn repeat_bin(v: &[u8], n: usize) -> ArrayRef {
    let mut data = Vec::with_capacity(v.len() * n);
    let mut off = Vec::with_capacity(n + 1);
    off.push(0i32);
    for _ in 0..n {
        data.extend_from_slice(v);
        off.push(data.len() as i32);
    }
    let off = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(off)) };
    Arc::new(BinaryArray::new(off, Buffer::from_vec(data), None))
}

pub fn prim<T: ArrowPrimitiveType>(v: Vec<T::Native>, dt: DataType) -> ArrayRef {
    Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(v), None).with_data_type(dt))
}

/// A `Map(String, String)` column: per-row entry offsets, keys and values.
#[derive(Default)]
pub struct Map {
    pub off: Vec<i32>,
    pub keys: Bin,
    pub vals: Bin,
}

impl Map {
    pub fn clear(&mut self) {
        self.off.clear();
        self.off.push(0);
        self.keys.clear();
        self.vals.clear();
    }
    /// Ends one map (row, or list element).
    #[inline]
    pub fn commit(&mut self) {
        self.off.push(self.keys.len() as i32);
    }
    pub fn take(&mut self, entries: &FieldRef) -> ArrayRef {
        Arc::new(self.take_map(entries))
    }
    pub fn take_map(&mut self, entries: &FieldRef) -> MapArray {
        let off = std::mem::replace(&mut self.off, vec![0]);
        let off = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(off)) };
        let DataType::Struct(fields) = entries.data_type() else { unreachable!() };
        let st = StructArray::new(fields.clone(), vec![self.keys.take(), self.vals.take()], None);
        MapArray::new(entries.clone(), off, st, None, false)
    }
}

/// List offsets for a list column.
#[derive(Default)]
pub struct ListOff {
    pub off: Vec<i32>,
}

impl ListOff {
    pub fn clear(&mut self) {
        self.off.clear();
        self.off.push(0);
    }
    #[inline]
    pub fn commit(&mut self, inner_len: usize) {
        self.off.push(inner_len as i32);
    }
    pub fn take(&mut self, element: &FieldRef, values: ArrayRef) -> ArrayRef {
        let off = std::mem::replace(&mut self.off, vec![0]);
        let off = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(off)) };
        Arc::new(ListArray::new(element.clone(), off, values, None))
    }
}

/// The `key_value` entries struct of a map column, with Binary or Utf8 leaves.
pub fn map_entries(string: &DataType) -> FieldRef {
    Arc::new(Field::new(
        "key_value",
        DataType::Struct(Fields::from(vec![
            Field::new("key", string.clone(), false),
            Field::new("value", string.clone(), false),
        ])),
        false,
    ))
}
