//! A minimal Parquet writer: PLAIN encoding, uncompressed, DataPageV2, one
//! row group and one page per column. Exactly what the two output tables
//! need, nothing more.

use std::io::Write;

// ---------------------------------------------------------------- schema

#[derive(Clone, Copy, PartialEq)]
pub enum PType {
    Int32,
    Int64,
    ByteArray,
}

impl PType {
    fn thrift(self) -> i32 {
        match self {
            PType::Int32 => 1,
            PType::Int64 => 2,
            PType::ByteArray => 6,
        }
    }
}

#[derive(Clone, Copy)]
pub enum PLogical {
    None,
    Str,
    Date,
}

pub struct Field {
    pub name: &'static str,
    pub ptype: PType,
    pub logical: PLogical,
    pub optional: bool,
}

pub enum ColData<'a> {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    /// Strings; `defs[i] == false` means null (only allowed for optional).
    Str {
        vals: Vec<&'a [u8]>,
        defs: Vec<bool>,
    },
}

// ---------------------------------------------------------------- thrift

const CT_TRUE: u8 = 1;
const CT_FALSE: u8 = 2;
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

struct Tw {
    out: Vec<u8>,
    last_id: Vec<i16>,
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

impl Tw {
    fn new() -> Self {
        Tw {
            out: Vec::new(),
            last_id: vec![0],
        }
    }

    fn begin(&mut self) {
        self.last_id.push(0);
    }

    fn end(&mut self) {
        self.out.push(0);
        self.last_id.pop();
    }

    fn field_header(&mut self, id: i16, ct: u8) {
        let last = *self.last_id.last().unwrap();
        let delta = id - last;
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | ct);
        } else {
            self.out.push(ct);
            varint(((id as i32) << 1 ^ ((id as i32) >> 31)) as u32 as u64, &mut self.out);
        }
        *self.last_id.last_mut().unwrap() = id;
    }

    fn i32(&mut self, id: i16, v: i32) {
        self.field_header(id, CT_I32);
        varint(((v as i64) << 1 ^ ((v as i64) >> 63)) as u64, &mut self.out);
    }

    fn i64(&mut self, id: i16, v: i64) {
        self.field_header(id, CT_I64);
        varint(((v as u64) << 1) ^ (((v as i64) >> 63) as u64), &mut self.out);
    }

    fn binary(&mut self, id: i16, v: &[u8]) {
        self.field_header(id, CT_BINARY);
        varint(v.len() as u64, &mut self.out);
        self.out.extend_from_slice(v);
    }

    fn boolean(&mut self, id: i16, v: bool) {
        self.field_header(id, if v { CT_TRUE } else { CT_FALSE });
    }

    /// Begin a list field; elements are written right after with raw_* calls.
    fn list(&mut self, id: i16, elem_ct: u8, len: usize) {
        self.field_header(id, CT_LIST);
        if len <= 14 {
            self.out.push(((len as u8) << 4) | elem_ct);
        } else {
            self.out.push(0xf0 | elem_ct);
            varint(len as u64, &mut self.out);
        }
    }

    fn raw_i32(&mut self, v: i32) {
        varint(((v as i64) << 1 ^ ((v as i64) >> 63)) as u64, &mut self.out);
    }

    fn raw_binary(&mut self, v: &[u8]) {
        varint(v.len() as u64, &mut self.out);
        self.out.extend_from_slice(v);
    }

    /// An empty-struct union member (String, Date) under `parent_id`.
    fn empty_union_member(&mut self, parent_id: i16, member_id: i16) {
        self.field_header(parent_id, CT_STRUCT);
        self.begin();
        self.field_header(member_id, CT_STRUCT);
        self.begin();
        self.end();
        self.end();
    }
}

// ---------------------------------------------------------------- levels

fn rle_defs(defs: &[bool]) -> Vec<u8> {
    let mut out = Vec::with_capacity(defs.len() / 8 + 8);
    let mut i = 0;
    while i < defs.len() {
        let v = defs[i];
        let mut n = 1usize;
        while i + n < defs.len() && defs[i + n] == v {
            n += 1;
        }
        varint((n as u64) << 1, &mut out);
        out.push(u8::from(v));
        i += n;
    }
    out
}

// ---------------------------------------------------------------- writer

/// Write one parquet file with the given schema and columns.
pub fn write_parquet<'a>(path: &str, fields: &[Field], cols: &[ColData<'a>], rows: usize) {
    debug_assert_eq!(fields.len(), cols.len());
    let mut out: Vec<u8> = Vec::with_capacity(64 * 1024 * 1024);
    out.extend_from_slice(b"PAR1");

    let mut chunk_meta: Vec<(i64, i64)> = Vec::with_capacity(cols.len());
    for (field, col) in fields.iter().zip(cols) {
        let body = build_page(field, col, rows);
        let header = page_header(field, rows, body.0, body.1, body.2.len());
        let offset = out.len() as i64;
        out.extend_from_slice(&header);
        out.extend_from_slice(&body.2);
        chunk_meta.push((offset, (header.len() + body.2.len()) as i64));
    }

    let footer = file_metadata(fields, rows, &chunk_meta);
    out.extend_from_slice(&footer);
    out.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");

    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(&out).unwrap();
}

/// Returns (num_nulls, def_len, page_body).
fn build_page<'a>(field: &Field, col: &ColData<'a>, rows: usize) -> (i32, usize, Vec<u8>) {
    let mut body = Vec::new();
    let mut num_nulls = 0i32;
    match col {
        ColData::Int32(v) => {
            debug_assert_eq!(v.len(), rows);
            for &x in v {
                body.extend_from_slice(&x.to_le_bytes());
            }
        }
        ColData::Int64(v) => {
            debug_assert_eq!(v.len(), rows);
            for &x in v {
                body.extend_from_slice(&x.to_le_bytes());
            }
        }
        ColData::Str { vals, defs } => {
            debug_assert_eq!(vals.len(), rows);
            debug_assert_eq!(defs.len(), rows);
            if field.optional {
                let dl = rle_defs(defs);
                body.extend_from_slice(&dl);
                let dl_len = dl.len();
                for (i, s) in vals.iter().enumerate() {
                    if defs[i] {
                        body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        body.extend_from_slice(s);
                    } else {
                        num_nulls += 1;
                    }
                }
                return (num_nulls, dl_len, body);
            }
            for s in vals {
                body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                body.extend_from_slice(s);
            }
        }
    }
    (0, 0, body)
}

/// `page_len` is the full page body length (def levels + values).
fn page_header(_field: &Field, rows: usize, num_nulls: i32, def_len: usize, page_len: usize) -> Vec<u8> {
    let total = page_len;
    let mut w = Tw::new();
    w.begin();
    w.i32(1, 3); // DATA_PAGE_V2
    w.i32(2, total as i32); // uncompressed_page_size
    w.i32(3, total as i32); // compressed_page_size
    w.field_header(8, CT_STRUCT); // data_page_header_v2
    w.begin();
    w.i32(1, rows as i32); // num_values
    w.i32(2, num_nulls); // num_nulls
    w.i32(3, rows as i32); // num_rows
    w.i32(4, 0); // PLAIN
    w.i32(5, def_len as i32); // definition_levels_byte_length
    w.i32(6, 0); // repetition_levels_byte_length
    w.boolean(7, false); // is_compressed (UNCOMPRESSED codec)
    w.end();
    w.end();
    w.out
}

fn file_metadata(fields: &[Field], rows: usize, chunk_meta: &[(i64, i64)]) -> Vec<u8> {
    let mut w = Tw::new();
    w.begin();
    w.i32(1, 1); // version
    // schema
    w.list(2, CT_STRUCT, fields.len() + 1);
    schema_element_root(&mut w, fields.len());
    for f in fields {
        schema_element_leaf(&mut w, f);
    }
    w.i64(3, rows as i64);
    // row groups (always present, one when there are rows)
    w.list(4, CT_STRUCT, usize::from(rows > 0));
    if rows > 0 {
        w.begin(); // RowGroup
        w.list(1, CT_STRUCT, fields.len());
        let mut total_size = 0i64;
        for (f, &(off, size)) in fields.iter().zip(chunk_meta) {
            total_size += size;
            w.begin(); // ColumnChunk
            w.i64(2, off);
            w.field_header(3, CT_STRUCT); // meta_data
            w.begin();
            w.i32(1, f.ptype.thrift());
            // encodings
            if f.optional {
                w.list(2, CT_I32, 2);
                w.raw_i32(0); // PLAIN
                w.raw_i32(3); // RLE
            } else {
                w.list(2, CT_I32, 1);
                w.raw_i32(0);
            }
            w.list(3, CT_BINARY, 1);
            w.raw_binary(f.name.as_bytes());
            w.i32(4, 0); // codec UNCOMPRESSED
            w.i64(5, rows as i64); // num_values
            w.i64(6, size); // total_uncompressed_size
            w.i64(7, size); // total_compressed_size
            w.i64(9, off); // data_page_offset
            w.end();
            w.end(); // ColumnChunk
        }
        w.i64(2, total_size);
        w.i64(3, rows as i64);
        w.end(); // RowGroup
    }
    w.binary(6, b"shopstats 1.0");
    w.end();
    w.out
}

fn schema_element_root(w: &mut Tw, children: usize) {
    w.begin();
    w.binary(4, b"schema"); // name
    w.i32(5, children as i32); // num_children
    w.end();
}

fn schema_element_leaf(w: &mut Tw, f: &Field) {
    w.begin();
    w.i32(1, f.ptype.thrift()); // type
    w.i32(3, if f.optional { 1 } else { 0 }); // repetition
    w.binary(4, f.name.as_bytes()); // name
    match f.logical {
        PLogical::Str => {
            w.i32(6, 0); // converted_type UTF8
            w.empty_union_member(10, 1); // logicalType STRING
        }
        PLogical::Date => {
            w.i32(6, 6); // converted_type DATE
            w.empty_union_member(10, 6); // logicalType DATE
        }
        PLogical::None => {}
    }
    w.end();
}
