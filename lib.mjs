import { ptr, dlopen, CString, toArrayBuffer } from 'bun:ffi';

const utf8e = new TextEncoder();
const GeneratorFunction = (function* () { }).constructor;

const path = {
  linux() { return new URL(`./bin/libduckdb.so`, import.meta.url); },
  darwin() { return new URL(`./bin/libduckdb.dylib`, import.meta.url); },
}[process.platform]().pathname;

const duck = dlopen(path, {
  duckffi_dfree: { args: ['ptr'], returns: 'void' },
  duckffi_close: { args: ['ptr'], returns: 'void' },
  duckffi_connect: { args: ['ptr'], returns: 'ptr' },
  duckffi_row_count: { args: ['ptr'], returns: 'u32' },
  duckffi_enum_size: { args: ['ptr'], returns: 'u32' },
  duckffi_enum_type: { args: ['ptr'], returns: 'u32' },
  duckffi_blob_size: { args: ['ptr'], returns: 'u32' },
  duckffi_blob_data: { args: ['ptr'], returns: 'ptr' },
  duckffi_free_blob: { args: ['ptr'], returns: 'void' },
  duckffi_free_ltype: { args: ['ptr'], returns: 'void' },
  duckffi_disconnect: { args: ['ptr'], returns: 'void' },
  duckffi_param_count: { args: ['ptr'], returns: 'u32' },
  duckffi_open: { args: ['bool', 'ptr'], returns: 'ptr' },
  duckffi_query: { args: ['ptr', 'ptr'], returns: 'ptr' },
  duckffi_free_result: { args: ['ptr'], returns: 'void' },
  duckffi_column_count: { args: ['ptr'], returns: 'u32' },
  duckffi_result_error: { args: ['ptr'], returns: 'ptr' },
  duckffi_free_prepare: { args: ['ptr'], returns: 'void' },
  duckffi_prepare_error: { args: ['ptr'], returns: 'ptr' },
  duckffi_prepare: { args: ['ptr', 'ptr'], returns: 'ptr' },
  duckffi_row_count_slow: { args: ['ptr'], returns: 'u64' },
  duckffi_query_prepared: { args: ['ptr'], returns: 'ptr' },
  duckffi_row_count_large: { args: ['ptr'], returns: 'bool' },
  duckffi_param_type: { args: ['ptr', 'u32'], returns: 'u32' },
  duckffi_enum_string: { args: ['ptr', 'u32'], returns: 'ptr' },
  duckffi_column_name: { args: ['ptr', 'u32'], returns: 'ptr' },
  duckffi_column_type: { args: ['ptr', 'u32'], returns: 'u32' },
  duckffi_column_ltype: { args: ['ptr', 'u32'], returns: 'ptr' },

  duckffi_bind_null: { args: ['ptr', 'u32'], returns: 'bool' },
  duckffi_bind_u8: { args: ['ptr', 'u32', 'u8'], returns: 'bool' },
  duckffi_bind_i8: { args: ['ptr', 'u32', 'i8'], returns: 'bool' },
  duckffi_bind_u16: { args: ['ptr', 'u32', 'u16'], returns: 'bool' },
  duckffi_bind_i16: { args: ['ptr', 'u32', 'i16'], returns: 'bool' },
  duckffi_bind_u32: { args: ['ptr', 'u32', 'u32'], returns: 'bool' },
  duckffi_bind_i32: { args: ['ptr', 'u32', 'i32'], returns: 'bool' },
  duckffi_bind_f32: { args: ['ptr', 'u32', 'f32'], returns: 'bool' },
  duckffi_bind_u64: { args: ['ptr', 'u32', 'u64'], returns: 'bool' },
  duckffi_bind_i64: { args: ['ptr', 'u32', 'i64'], returns: 'bool' },
  duckffi_bind_f64: { args: ['ptr', 'u32', 'f64'], returns: 'bool' },
  duckffi_bind_blob: { args: ['ptr', 'u32', 'ptr', 'u32'], returns: 'bool' },
  duckffi_bind_string: { args: ['ptr', 'u32', 'ptr', 'u32'], returns: 'bool' },
  duckffi_bind_timestamp: { args: ['ptr', 'u32', 'u64_fast'], returns: 'bool' },
  duckffi_bind_interval: { args: ['ptr', 'u32', 'u32', 'u32', 'u32'], returns: 'bool' },

  duckffi_value_u8: { args: ['ptr', 'u32', 'u32'], returns: 'u8' },
  duckffi_value_i8: { args: ['ptr', 'u32', 'u32'], returns: 'i8' },
  duckffi_value_u16: { args: ['ptr', 'u32', 'u32'], returns: 'u16' },
  duckffi_value_i16: { args: ['ptr', 'u32', 'u32'], returns: 'i16' },
  duckffi_value_u32: { args: ['ptr', 'u32', 'u32'], returns: 'u32' },
  duckffi_value_i32: { args: ['ptr', 'u32', 'u32'], returns: 'i32' },
  duckffi_value_f32: { args: ['ptr', 'u32', 'u32'], returns: 'f32' },
  duckffi_value_u64: { args: ['ptr', 'u32', 'u32'], returns: 'u64' },
  duckffi_value_i64: { args: ['ptr', 'u32', 'u32'], returns: 'i64' },
  duckffi_value_f64: { args: ['ptr', 'u32', 'u32'], returns: 'f64' },
  duckffi_value_time: { args: ['ptr', 'u32', 'u32'], returns: 'u32' },
  duckffi_value_date: { args: ['ptr', 'u32', 'u32'], returns: 'u32' },
  duckffi_value_blob: { args: ['ptr', 'u32', 'u32'], returns: 'ptr' },
  duckffi_value_string: { args: ['ptr', 'u32', 'u32'], returns: 'ptr' },
  duckffi_value_boolean: { args: ['ptr', 'u32', 'u32'], returns: 'bool' },
  duckffi_value_is_null: { args: ['ptr', 'u32', 'u32'], returns: 'bool' },
  duckffi_value_interval_days: { args: ['ptr', 'u32', 'u32'], returns: 'u32' },
  duckffi_value_interval_months: { args: ['ptr', 'u32', 'u32'], returns: 'u32' },
  duckffi_value_timestamp_ms: { args: ['ptr', 'u32', 'u32'], returns: 'u64_fast' },

  duckffi_value_u8_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u8' },
  duckffi_value_i8_slow: { args: ['ptr', 'u64', 'u32'], returns: 'i8' },
  duckffi_value_u16_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u16' },
  duckffi_value_i16_slow: { args: ['ptr', 'u64', 'u32'], returns: 'i16' },
  duckffi_value_u32_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u32' },
  duckffi_value_i32_slow: { args: ['ptr', 'u64', 'u32'], returns: 'i32' },
  duckffi_value_f32_slow: { args: ['ptr', 'u64', 'u32'], returns: 'f32' },
  duckffi_value_u64_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u64' },
  duckffi_value_i64_slow: { args: ['ptr', 'u64', 'u32'], returns: 'i64' },
  duckffi_value_f64_slow: { args: ['ptr', 'u64', 'u32'], returns: 'f64' },
  duckffi_value_time_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u32' },
  duckffi_value_date_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u32' },
  duckffi_value_blob_slow: { args: ['ptr', 'u64', 'u32'], returns: 'ptr' },
  duckffi_value_string_slow: { args: ['ptr', 'u64', 'u32'], returns: 'ptr' },
  duckffi_value_boolean_slow: { args: ['ptr', 'u64', 'u32'], returns: 'bool' },
  duckffi_value_is_null_slow: { args: ['ptr', 'u64', 'u32'], returns: 'bool' },
  duckffi_value_interval_days_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u32' },
  duckffi_value_interval_months_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u32' },
  duckffi_value_timestamp_ms_slow: { args: ['ptr', 'u64', 'u32'], returns: 'u64_fast' },
}).symbols;

for (const k in duck) duck[k] = duck[k].native || duck[k];

export function close(db) {
  duck.duckffi_close(db);
}

export function disconnect(c) {
  duck.duckffi_disconnect(c);
}

export function connect(db) {
  const c = duck.duckffi_connect(db);
  if (0 === c) throw new Error('duckdb: failed to connect to database'); return c;
}

export function open(path) {
  const db = path === null
    ? duck.duckffi_open(true, 0)
    : duck.duckffi_open(false, ptr(utf8e.encode(path + '\0')));

  if (0 === db) throw new Error('duckdb: failed to open database'); return db;
}

const _t = {
  invalid: 0,
  boolean: 1,
  tinyint: 2,
  smallint: 3,
  integer: 4,
  bigint: 5,
  utinyint: 6,
  usmallint: 7,
  uinteger: 8,
  ubigint: 9,
  float: 10,
  double: 11,
  timestamp: 12,
  date: 13,
  time: 14,
  interval: 15,
  hugeint: 16,
  varchar: 17,
  blob: 18,
  decimal: 19,
  timestamp_s: 20,
  timestamp_ms: 21,
  timestamp_ns: 22,
  enum: 23,
  list: 24,
  struct: 25,
  map: 26,
  uuid: 27,
  json: 28,
};

const _tr = Object.fromEntries(Object.entries(_t).map(([k, v]) => [v, k]));
const blob_gc = new FinalizationRegistry(ptr => duck.duckffi_free_blob(ptr));
const ltype_gc = new FinalizationRegistry(ptr => duck.duckffi_free_ltype(ptr));
const result_gc = new FinalizationRegistry(ptr => duck.duckffi_free_result(ptr));
const prepare_gc = new FinalizationRegistry(ptr => duck.duckffi_free_prepare(ptr));

const _tm = {
  [_t.time](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_time(r, row, column); },
  [_t.float](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_f32(r, row, column); },
  [_t.double](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_f64(r, row, column); },
  [_t.bigint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i64(r, row, column); },
  [_t.tinyint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i8(r, row, column); },
  [_t.ubigint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u64(r, row, column); },
  [_t.integer](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i32(r, row, column); },
  [_t.utinyint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u8(r, row, column); },
  [_t.smallint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i16(r, row, column); },
  [_t.uinteger](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u32(r, row, column); },
  [_t.usmallint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u16(r, row, column); },
  [_t.boolean](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_boolean(r, row, column); },
  [_t.timestamp](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_timestamp_ms(r, row, column); },
  [_t.varchar](r, _ltypes, _column) { return (row, column) => new CString(duck.duckffi_value_string(r, row, column)); },
  [_t.date](r, _ltypes, _column) { return (row, column) => 24 * 60 * 60 * 1000 * duck.duckffi_value_date(r, row, column); },

  [_t.blob](r, _ltypes, _column) {
    return (row, column) => {
      const blob = duck.duckffi_value_blob(r, row, column);
      const ab = toArrayBuffer(duck.duckffi_blob_data(blob), 0, duck.duckffi_blob_size(blob));

      return (blob_gc.register(ab, blob), new Uint8Array(ab));
    };
  },

  [_t.interval](r, _ltypes, _column) {
    return (row, column) => {
      const ms = duck.duckffi_value_interval_days(r, row, column);

      const days = (ms / (24 * 60 * 60 * 1000)) | 0;

      return {
        days: days,
        ms: ms - days * (24 * 60 * 60 * 1000),
        months: duck.duckffi_value_interval_months(r, row, column),
      };
    };
  },

  [_t.enum](r, ltypes, _column) {
    const ltype = ltypes[_column] = duck.duckffi_column_ltype(r, _column);

    const names = new Array(duck.duckffi_enum_size(ltype));
    const tf = _tm[duck.duckffi_enum_type(ltype)](r, ltypes, _column);

    return (row, column) => {
      const offset = tf(row, column);

      let name = names[offset];

      if (name === undefined) {
        const s = duck.duckffi_enum_string(ltype, offset);
        name = names[offset] = new CString(s); duck.duckffi_dfree(name.ptr);
      }

      return name;
    };
  },
};

const _tms = {
  [_t.time](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_time_slow(r, row, column); },
  [_t.float](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_f32_slow(r, row, column); },
  [_t.double](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_f64_slow(r, row, column); },
  [_t.bigint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i64_slow(r, row, column); },
  [_t.tinyint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i8_slow(r, row, column); },
  [_t.ubigint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u64_slow(r, row, column); },
  [_t.integer](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i32_slow(r, row, column); },
  [_t.utinyint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u8_slow(r, row, column); },
  [_t.smallint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_i16_slow(r, row, column); },
  [_t.uinteger](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u32_slow(r, row, column); },
  [_t.usmallint](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_u16_slow(r, row, column); },
  [_t.boolean](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_boolean_slow(r, row, column); },
  [_t.timestamp](r, _ltypes, _column) { return (row, column) => duck.duckffi_value_timestamp_ms_slow(r, row, column); },
  [_t.varchar](r, _ltypes, _column) { return (row, column) => new CString(duck.duckffi_value_string_slow(r, row, column)); },
  [_t.date](r, _ltypes, _column) { return (row, column) => 24 * 60 * 60 * 1000 * duck.duckffi_value_date_slow(r, row, column); },

  [_t.blob](r, _ltypes, _column) {
    return (row, column) => {
      const blob = duck.duckffi_value_blob_slow(r, row, column);
      const ab = toArrayBuffer(duck.duckffi_blob_data(blob), 0, duck.duckffi_blob_size(blob));

      return (blob_gc.register(ab, blob), new Uint8Array(ab));
    };
  },

  [_t.interval](r, _ltypes, _column) {
    return (row, column) => {
      const ms = duck.duckffi_value_interval_days_slow(r, row, column);

      const days = (ms / (24 * 60 * 60 * 1000)) | 0;

      return {
        days: days,
        ms: ms - days * (24 * 60 * 60 * 1000),
        months: duck.duckffi_value_interval_months_slow(r, row, column),
      };
    };
  },

  [_t.enum](r, ltypes, _column) {
    const ltype = ltypes[_column] = duck.duckffi_column_ltype(r, _column);

    const names = new Array(duck.duckffi_enum_size(ltype));
    const tf = _tms[duck.duckffi_enum_type(ltype)](r, ltypes, _column);

    return (row, column) => {
      const offset = tf(row, column);

      let name = names[offset];

      if (name === undefined) {
        const s = duck.duckffi_enum_string(ltype, offset);
        name = names[offset] = new CString(s); duck.duckffi_dfree(name.ptr);
      }

      return name;
    };
  },
};

export function query(c, query) {
  const r = duck.duckffi_query(c, ptr(utf8e.encode(query + '\0')));

  {
    const e = duck.duckffi_result_error(r);

    if (e) {
      const s = new CString(e);
      throw (duck.duckffi_free_result(r), new Error(s));
    }
  }

  const rows = duck.duckffi_row_count(r);
  const columns = duck.duckffi_column_count(r);

  const a = new Array(rows);
  const names = new Array(columns);
  const types = new Array(columns);
  const ltypes = new Array(columns);

  try {
    for (let offset = 0; offset < columns; offset++) {
      names[offset] = new CString(duck.duckffi_column_name(r, offset));
      types[offset] = _tm[duck.duckffi_column_type(r, offset)](r, ltypes, offset);
    }

    for (let offset = 0; rows > offset; offset++) {
      const row = a[offset] = {};

      for (let column = 0; column < columns; column++) {
        if (duck.duckffi_value_is_null(r, offset, column)) {
          row[names[column]] = null;
        } else {
          row[names[column]] = types[column](offset, column);
        }
      }
    }

    return a;
  } finally {
    const len = ltypes.length;

    for (let offset = 0; len > offset; offset++) {
      const x = ltypes[offset];
      if (x) duck.duckffi_free_ltype(x);
    }

    duck.duckffi_free_result(r);
  }
}

export function* stream(c, query) {
  const r = duck.duckffi_query(c, ptr(utf8e.encode(query + '\0')));

  {
    const e = duck.duckffi_result_error(r);

    if (e) {
      const s = new CString(e);
      throw (duck.duckffi_free_result(r), new Error(s));
    }
  }

  const t = {};
  result_gc.register(t, r, t);
  const slow = duck.duckffi_row_count_large(r);
  const columns = duck.duckffi_column_count(r);

  const names = new Array(columns);
  const types = new Array(columns);
  const ltypes = new Array(columns);

  for (let offset = 0; offset < columns; offset++) {
    names[offset] = new CString(duck.duckffi_column_name(r, offset));
    types[offset] = (!slow ? _tm : _tms)[duck.duckffi_column_type(r, offset)](r, ltypes, offset);
  }

  const len = ltypes.length;

  for (let offset = 0; len > offset; offset++) {
    const ltype = ltypes[offset];
    if (ltype) ltype_gc.register(t, ltype, t);
  }

  if (!slow) {
    const rows = duck.duckffi_row_count(r);

    for (let offset = 0; rows > offset; offset++) {
      const row = {};

      for (let column = 0; column < columns; column++) {
        if (duck.duckffi_value_is_null(r, offset, column)) {
          row[names[column]] = null;
        } else {
          row[names[column]] = types[column](offset, column);
        }
      }

      yield row;
    }
  } else {
    const rows = duck.duckffi_row_count_slow(r);

    for (let offset = 0n; rows > offset; offset++) {
      const row = {};

      for (let column = 0; column < columns; column++) {
        if (duck.duckffi_value_is_null_slow(r, offset, column)) {
          row[names[column]] = null;
        } else {
          row[names[column]] = types[column](offset, column);
        }
      }

      yield row;
    }
  }

  for (let offset = 0; len > offset; offset++) {
    const x = ltypes[offset];
    if (x) duck.duckffi_free_ltype(x);
  }

  ltype_gc.unregister(t);
  result_gc.unregister(t);
  duck.duckffi_free_result(r);
}

const _trm = {
  [_t.enum](n, offset) { return _trm[_t.varchar](n, offset); },
  [_t.time](n, offset) { return `if (type === 'string') { ${_trm[_t.varchar](n, offset)} } else { ${_trm[_t.timestamp](n, offset)} }`; },
  [_t.date](n, offset) { return `if (type === 'string') { ${_trm[_t.varchar](n, offset)} } else { ${_trm[_t.timestamp](n, offset)} }`; },
  [_t.float](n, offset) { return `if (duck.duckffi_bind_f32(p, ${offset}, ${n})) throw new Error('failed to bind float at ${offset}');`; },
  [_t.bigint](n, offset) { return `if (duck.duckffi_bind_i64(p, ${offset}, ${n})) throw new Error('failed to bind bigint at ${offset}');`; },
  [_t.double](n, offset) { return `if (duck.duckffi_bind_f64(p, ${offset}, ${n})) throw new Error('failed to bind double at ${offset}');`; },
  [_t.ubigint](n, offset) { return `if (duck.duckffi_bind_u64(p, ${offset}, ${n})) throw new Error('failed to bind ubigint at ${offset}');`; },
  [_t.boolean](n, offset) { return `if (duck.duckffi_bind_bool(p, ${offset}, ${n})) throw new Error('failed to bind boolean at ${offset}');`; },
  [_t.tinyint](n, offset) { return `if (duck.duckffi_bind_i8(p, ${offset}, ${n} | 0)) throw new Error('failed to bind tinyint at ${offset}');`; },
  [_t.integer](n, offset) { return `if (duck.duckffi_bind_i32(p, ${offset}, ${n} | 0)) throw new Error('failed to bind integer at ${offset}');`; },
  [_t.utinyint](n, offset) { return `if (duck.duckffi_bind_u8(p, ${offset}, ${n} | 0)) throw new Error('failed to bind utinyint at ${offset}');`; },
  [_t.smallint](n, offset) { return `if (duck.duckffi_bind_i16(p, ${offset}, ${n} | 0)) throw new Error('failed to bind smallint at ${offset}');`; },
  [_t.usmallint](n, offset) { return `if (duck.duckffi_bind_u16(p, ${offset}, ${n} | 0)) throw new Error('failed to bind usmallint at ${offset}');`; },
  [_t.uinteger](n, offset) { return `if (duck.duckffi_bind_u32(p, ${offset}, ${n} >>> 0)) throw new Error('failed to bind uinteger at ${offset}');`; },
  [_t.timestamp](n, offset) { return `if (duck.duckffi_bind_timestamp(p, ${offset}, ${n})) throw new Error('failed to bind timestamp at ${offset}');`; },
  [_t.blob](n, offset) { return `if (duck.duckffi_bind_blob(p, ${offset}, ptr(${n}), ${n}.byteLength)) throw new Error('failed to bind blob at ${offset}');`; },

  [_t.varchar](n, offset) {
    return `
      const ${n}_utf8 = utf8e.encode(${n});
      if (duck.duckffi_bind_string(p, ${offset}, ptr(${n}_utf8), ${n}_utf8.length)) throw new Error('failed to bind varchar at ${offset}');
    `;
  },

  [_t.interval](n, offset) {
    return `
      if (type === 'string') {
        ${_trm[_t.varchar](n, offset)};
      } else {
        if (duck.duckffi_bind_interval(p, ${offset}, ${n}.ms, ${n}.days, ${n}.months)) throw new Error('failed to bind interval at ${offset}');
      }
    `;
  },
};

const _trmc = {
  [_t.enum](n) { return `(type === 'string')`; },
  [_t.float](n) { return `(type === 'number')`; },
  [_t.double](n) { return `(type === 'number')`; },
  [_t.varchar](n) { return `(type === 'string')`; },
  [_t.boolean](n) { return `(type === 'boolean')`; },
  [_t.time](n) { return `(type === 'string') || (type === 'number')`; },
  [_t.date](n) { return `(type === 'number') || (type === 'string')`; },
  [_t.timestamp](n) { return `(type === 'number') || (type === 'bigint')`; },
  [_t.utinyint](n) { return `(type === 'number') && (0 <= ${n}) && (${n} <= 255)`; },
  [_t.tinyint](n) { return `(type === 'number') && (${n} <= 127) && (${n} >= -127)`; },
  [_t.usmallint](n) { return `(type === 'number') && (0 <= ${n}) && (${n} <= 65535)`; },
  [_t.blob](n) { return `(ArrayBuffer.isView(${n})) || (${n} instanceof ArrayBuffer)`; },
  [_t.uinteger](n) { return `(type === 'number') && (0 <= ${n}) && (${n} <= 4294967295)`; },
  [_t.smallint](n) { return `(type === 'number') && (${n} <= 32767) && (${n} >= -32767)`; },
  [_t.integer](n) { return `(type === 'number') && (${n} <= 2147483647) && (${n} >= -2147483647)`; },
  [_t.ubigint](n) { return `(type === 'bigint') && (0n <= ${n}) && (${n} <= 18446744073709551615n)`; },
  [_t.bigint](n) { return `(type === 'bigint') && (${n} <= 9223372036854775807n) && (${n} >= -9223372036854775807n)`; },
  [_t.interval](n) { return `(type === 'string') || ((type === 'object') && ('number' === typeof ${n}.ms) && ('number' === typeof ${n}.days) && ('number' === typeof ${n}.months))`; },
};

export function prepare(c, query) {
  const p = duck.duckffi_prepare(c, ptr(utf8e.encode(query + '\0')));

  {
    const e = duck.duckffi_prepare_error(p);

    if (e) {
      const s = new CString(e);
      throw (duck.duckffi_free_prepare(p), new Error(s));
    }
  }

  const ctx = {};
  prepare_gc.register(ctx, p);
  const len = duck.duckffi_param_count(p);

  const types = new Array(len);
  const names = new Array(len);

  for (let offset = 0; len > offset; offset++) {
    types[offset] = duck.duckffi_param_type(p, offset);
    names[offset] = `$${offset}_${_tr[types[offset]]}`;
  }

  ctx.query = new Function(...names, `
    const { ptr, CString } = Bun.FFI;
    const { p, _tm, ctx, duck, utf8e } = this;

    ${names.map((name, offset) => `
      if (null === ${name}) duck.duckffi_bind_null(p, ${offset});
      else {
        const type = typeof ${name};
        if (!(${_trmc[types[offset]](name)})) throw new TypeError('expected ${_tr[types[offset]]} at ${offset}');

        ${_trm[types[offset]](name, offset)}
      }
    `).join('\n')}

    const r = duck.duckffi_query_prepared(p);

    {
      const e = duck.duckffi_result_error(r);

      if (e) {
        const s = new CString(e);
        throw (duck.duckffi_free_result(r), new Error(s));
      }
    }

    const rows = duck.duckffi_row_count(r);
    const columns = duck.duckffi_column_count(r);

    const a = new Array(rows);
    const names = new Array(columns);
    const types = new Array(columns);
    const ltypes = new Array(columns);
    const typesfn = new Array(columns);

    for (let offset = 0; offset < columns; offset++) {
      types[offset] = duck.duckffi_column_type(r, offset);
      typesfn[offset] = _tm[types[offset]](r, ltypes, offset);
      names[offset] = new CString(duck.duckffi_column_name(r, offset));
    }

    try {
      {
        ctx.query = new Function(${names.map(n => `'${n}', `).join('')} \`
          const { ptr, CString } = Bun.FFI;
          const { p, _tm, duck, utf8e } = this;

          ${names.map((name, offset) => `
            if (null === ${name}) duck.duckffi_bind_null(p, ${offset});
            else {
              const type = typeof ${name};
              if (!(${_trmc[types[offset]](name)})) throw new TypeError('expected ${_tr[types[offset]]} at ${offset}');

              ${_trm[types[offset]](name, offset)}
            }
          `).join('\n')}

          const r = duck.duckffi_query_prepared(p);

          {
            const e = duck.duckffi_result_error(r);

            if (e) {
              const s = new CString(e);
              throw (duck.duckffi_free_result(r), new Error(s));
            }
          }

          const ltypes = new Array(\${ltypes.length});

          \${types.map((type, column) => \`
            const _tm_\${column}_\${type} = _tm[\${type}](r, ltypes, \${column});
          \`).join('\\n')}

          try {
            const rows = duck.duckffi_row_count(r);

            const a = new Array(rows);

            for (let offset = 0; rows > offset; offset++) {
              a[offset] = {
                \${names.map((name, column) => \`
                  "\${name}": duck.duckffi_value_is_null(r, offset, \${column}) ? null : _tm_\${column}_\${types[column]}(offset, \${column}),
                \`.trim()).join('\\n')}
              };
            }

            return a;
          } finally {
            for (let offset = 0; \${ltypes.length} > offset; offset++) {
              const x = ltypes[offset];
              if (x) duck.duckffi_free_ltype(x);
            }

            duck.duckffi_free_result(r);
          }
        \`).bind({ p, _tm, ctx, duck, utf8e });
      }

      for (let offset = 0; rows > offset; offset++) {
        const row = a[offset] = {};

        for (let column = 0; column < columns; column++) {
          if (duck.duckffi_value_is_null(r, offset, column)) {
            row[names[column]] = null;
          } else {
            row[names[column]] = typesfn[column](offset, column);
          }
        }
      }

      return a;
    } finally {
      const len = ltypes.length;

      for (let offset = 0; len > offset; offset++) {
        const x = ltypes[offset];
        if (x) duck.duckffi_free_ltype(x);
      }

      duck.duckffi_free_result(r);
    }
  `).bind({ p, _tm, ctx, duck, utf8e });



  ctx.stream = new GeneratorFunction(...names, `
    const { ptr, CString } = Bun.FFI;
    const GeneratorFunction = (function* () { }).constructor;
    const { p, _tm, ctx, _tms, duck, utf8e, ltype_gc, result_gc } = this;

    ${names.map((name, offset) => `
      if (null === ${name}) duck.duckffi_bind_null(p, ${offset});
      else {
        const type = typeof ${name};
        if (!(${_trmc[types[offset]](name)})) throw new TypeError('expected ${_tr[types[offset]]} at ${offset}');

        ${_trm[types[offset]](name, offset)}
      }
    `).join('\n')}

    const r = duck.duckffi_query_prepared(p);

    const t = {};
    result_gc.register(t, r, t);

    {
      const e = duck.duckffi_result_error(r);

      if (e) {
        const s = new CString(e);
        throw (duck.duckffi_free_result(r), new Error(s));
      }
    }

    const slow = duck.duckffi_row_count_large(r);
    const columns = duck.duckffi_column_count(r);

    const names = new Array(columns);
    const types = new Array(columns);
    const ltypes = new Array(columns);
    const typesfn = new Array(columns);

    for (let offset = 0; offset < columns; offset++) {
      types[offset] = duck.duckffi_column_type(r, offset);
      names[offset] = new CString(duck.duckffi_column_name(r, offset));
      typesfn[offset] = (!slow ? _tm : _tms)[types[offset]](r, ltypes, offset);
    }

    const len = ltypes.length;

    for (let offset = 0; len > offset; offset++) {
      const x = ltypes[offset];
      if (x) ltype_gc.register(t, x, t);
    }

    try {
      {
        ctx.stream = new GeneratorFunction(${names.map(n => `'${n}', `).join('')} \`
          const { ptr, CString } = Bun.FFI;
          const { p, _tm, _tms, duck, utf8e, ltype_gc, result_gc } = this;

          ${names.map((name, offset) => `
            if (null === ${name}) duck.duckffi_bind_null(p, ${offset});
            else {
              const type = typeof ${name};
              if (!(${_trmc[types[offset]](name)})) throw new TypeError('expected ${_tr[types[offset]]} at ${offset}');

              ${_trm[types[offset]](name, offset)}
            }
          `).join('\n')}

          const r = duck.duckffi_query_prepared(p);

          {
            const e = duck.duckffi_result_error(r);

            if (e) {
              const s = new CString(e);
              throw (duck.duckffi_free_result(r), new Error(s));
            }
          }

          const t = {};
          result_gc.register(t, r, t);
          const ltypes = new Array(\${len});
          const slow = duck.duckffi_row_count_large(r);

          try {
            if (!slow) {
              const rows = duck.duckffi_row_count(r);

              \${types.map((type, column) => \`
                const _tm_\${column}_\${type} = _tm[\${type}](r, ltypes, \${column});
              \`).join('\\n')}

              for (let offset = 0; \${len} > offset; offset++) {
                const x = ltypes[offset];
                if (x) ltype_gc.register(t, x, t);
              }

              for (let offset = 0; rows > offset; offset++) {
                yield {
                  \${names.map((name, column) => \`
                    "\${name}": duck.duckffi_value_is_null(r, offset, \${column}) ? null : _tm_\${column}_\${types[column]}(offset, \${column}),
                  \`.trim()).join('\\n')}
                };
              }
            } else {
              const rows = duck.duckffi_row_count_slow(r);

              \${types.map((type, column) => \`
                const _tms_\${column}_\${type} = _tms[\${type}](r, ltypes, \${column});
              \`).join('\\n')}

              for (let offset = 0; \${len} > offset; offset++) {
                const x = ltypes[offset];
                if (x) ltype_gc.register(t, x, t);
              }

              for (let offset = 0n; rows > offset; offset++) {
                yield {
                  \${names.map((name, column) => \`
                    "\${name}": duck.duckffi_value_is_null_slow(r, offset, \${column}) ? null : _tms_\${column}_\${types[column]}(offset, \${column}),
                  \`.trim()).join('\\n')}
                };
              }
            }
          } finally {
            for (let offset = 0; \${len} > offset; offset++) {
              const x = ltypes[offset];
              if (x) duck.duckffi_free_ltype(x);
            }

            ltype_gc.unregister(t);
            result_gc.unregister(t);
            duck.duckffi_free_result(r);
          }
        \`).bind({ p, _tm, ctx, _tms, duck, utf8e, ltype_gc, result_gc });
      }

      if (!slow) {
        const rows = duck.duckffi_row_count(r);

        for (let offset = 0; rows > offset; offset++) {
          const row = {};

          for (let column = 0; column < columns; column++) {
            if (duck.duckffi_value_is_null(r, offset, column)) {
              row[names[column]] = null;
            } else {
              row[names[column]] = typesfn[column](offset, column);
            }
          }

          yield row;
        }
      } else {
        const rows = duck.duckffi_row_count_slow(r);

        for (let offset = 0n; rows > offset; offset++) {
          const row = {};

          for (let column = 0; column < columns; column++) {
            if (duck.duckffi_value_is_null_slow(r, offset, column)) {
              row[names[column]] = null;
            } else {
              row[names[column]] = typesfn[column](offset, column);
            }
          }

          yield row;
        }
      }
    } finally {
      for (let offset = 0; len > offset; offset++) {
        const x = ltypes[offset];
        if (x) duck.duckffi_free_ltype(x);
      }

      ltype_gc.unregister(t);
      result_gc.unregister(t);
      duck.duckffi_free_result(r);
    }
  `).bind({ p, _tm, ctx, _tms, duck, utf8e, ltype_gc, result_gc });

  return ctx;
}