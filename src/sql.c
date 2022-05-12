#include <math.h>
#include <duckdb.h>
#include <string.h>

void duckffi_free(void* ptr) { free(ptr); }
void duckffi_dfree(void* ptr) { duckdb_free(ptr); }
void duckffi_free_blob(duckdb_blob* blob) { duckdb_free(blob->data); free(blob); }
void duckffi_close(duckdb_database db) { duckdb_database d = db; duckdb_close(&d); }
void duckffi_free_result(duckdb_result* result) { duckdb_destroy_result(result); free(result); }
void duckffi_disconnect(duckdb_connection con) { duckdb_connection c = con; duckdb_disconnect(&c); }
void duckffi_free_ltype(duckdb_logical_type ltype) { duckdb_logical_type t = ltype; duckdb_destroy_logical_type(&t); }
void duckffi_free_prepare(duckdb_prepared_statement prepare) { duckdb_prepared_statement p = prepare; duckdb_destroy_prepare(&p); }

duckdb_prepared_statement duckffi_prepare(duckdb_connection con, const char* query) {
  duckdb_prepared_statement p; duckdb_prepare(con, query, &p); return p;
}

duckdb_connection duckffi_connect(duckdb_database db) {
  duckdb_connection con; if (DuckDBError == duckdb_connect(db, &con)) return 0; return con;
}

duckdb_database duckffi_open(bool in_memory, const char* path) {
  duckdb_database db; if (DuckDBError == duckdb_open(in_memory ? NULL : path, &db)) return 0; return db;
}

duckdb_result* duckffi_query(duckdb_connection con, const char* query) {
  duckdb_result *res = (duckdb_result*)malloc(sizeof(duckdb_result)); duckdb_query(con, query, res); return res;
}

duckdb_result* duckffi_query_prepared(duckdb_prepared_statement prepare) {
  duckdb_result *res = (duckdb_result*)malloc(sizeof(duckdb_result)); duckdb_execute_prepared(prepare, res); return res;
}

void* duckffi_blob_data(duckdb_blob* blob) { return blob->data; }
uint32_t duckffi_blob_size(duckdb_blob* blob) { return blob->size; }
uint32_t duckffi_row_count(duckdb_result* result) { return duckdb_row_count(result); }
uint64_t duckffi_row_count_slow(duckdb_result* result) { return duckdb_row_count(result); }
uint32_t duckffi_column_count(duckdb_result* result) { return duckdb_column_count(result); }
const char* duckffi_result_error(duckdb_result* result) { return duckdb_result_error(result); }
uint32_t duckffi_enum_type(duckdb_logical_type type) { return duckdb_enum_internal_type(type); }
uint32_t duckffi_enum_size(duckdb_logical_type type) { return duckdb_enum_dictionary_size(type); }
uint32_t duckffi_param_count(duckdb_prepared_statement prepare) { return duckdb_nparams(prepare); }
bool duckffi_row_count_large(duckdb_result* result) { return 4294967296 < duckdb_row_count(result); }
const char* duckffi_prepare_error(duckdb_prepared_statement prepare) { return duckdb_prepare_error(prepare); }
uint32_t duckffi_column_type(duckdb_result* result, uint32_t offset) { return duckdb_column_type(result, offset); }
const char* duckffi_column_name(duckdb_result* result, uint32_t offset) { return duckdb_column_name(result, offset); }
char* duckffi_enum_string(duckdb_logical_type type, uint32_t offset) { return duckdb_enum_dictionary_value(type, offset); }
uint32_t duckffi_param_type(duckdb_prepared_statement prepare, uint32_t offset) { return duckdb_param_type(prepare, 1 + offset); }
duckdb_logical_type duckffi_column_ltype(duckdb_result* result, uint32_t offset) { return duckdb_column_logical_type(result, offset); }

bool duckffi_bind_null(duckdb_prepared_statement prepare, uint32_t offset) { return DuckDBError == duckdb_bind_null(prepare, 1 + offset); }
bool duckffi_bind_i8(duckdb_prepared_statement prepare, uint32_t offset, int8_t value) { return DuckDBError == duckdb_bind_int8(prepare, 1 + offset, value); }
bool duckffi_bind_f32(duckdb_prepared_statement prepare, uint32_t offset, float value) { return DuckDBError == duckdb_bind_float(prepare, 1 + offset, value); }
bool duckffi_bind_u8(duckdb_prepared_statement prepare, uint32_t offset, uint8_t value) { return DuckDBError == duckdb_bind_uint8(prepare, 1 + offset, value); }
bool duckffi_bind_i16(duckdb_prepared_statement prepare, uint32_t offset, int16_t value) { return DuckDBError == duckdb_bind_int16(prepare, 1 + offset, value); }
bool duckffi_bind_i32(duckdb_prepared_statement prepare, uint32_t offset, int32_t value) { return DuckDBError == duckdb_bind_int32(prepare, 1 + offset, value); }
bool duckffi_bind_i64(duckdb_prepared_statement prepare, uint32_t offset, int64_t value) { return DuckDBError == duckdb_bind_int64(prepare, 1 + offset, value); }
bool duckffi_bind_f64(duckdb_prepared_statement prepare, uint32_t offset, double value) { return DuckDBError == duckdb_bind_double(prepare, 1 + offset, value); }
bool duckffi_bind_u16(duckdb_prepared_statement prepare, uint32_t offset, uint16_t value) { return DuckDBError == duckdb_bind_uint16(prepare, 1 + offset, value); }
bool duckffi_bind_u32(duckdb_prepared_statement prepare, uint32_t offset, uint32_t value) { return DuckDBError == duckdb_bind_uint32(prepare, 1 + offset, value); }
bool duckffi_bind_u64(duckdb_prepared_statement prepare, uint32_t offset, uint64_t value) { return DuckDBError == duckdb_bind_uint64(prepare, 1 + offset, value); }
bool duckffi_bind_boolean(duckdb_prepared_statement prepare, uint32_t offset, bool value) { return DuckDBError == duckdb_bind_boolean(prepare, 1 + offset, value); }
bool duckffi_bind_blob(duckdb_prepared_statement prepare, uint32_t offset, const void* value, uint32_t length) { return DuckDBError == duckdb_bind_blob(prepare, 1 + offset, value, length); }
bool duckffi_bind_string(duckdb_prepared_statement prepare, uint32_t offset, const char* value, uint32_t length) { return DuckDBError == duckdb_bind_varchar_length(prepare, 1 + offset, value, length); }
bool duckffi_bind_timestamp(duckdb_prepared_statement prepare, uint32_t offset, uint64_t value) { duckdb_timestamp timestamp; timestamp.micros = 1000 * value; return DuckDBError == duckdb_bind_timestamp(prepare, 1 + offset, timestamp); }
bool duckffi_bind_interval(duckdb_prepared_statement prepare, uint32_t offset, uint32_t ms, uint32_t days, uint32_t months) { duckdb_interval interval; interval.days = days; interval.months = months; interval.micros = ms * 1000; return DuckDBError == duckdb_bind_interval(prepare, 1 + offset, interval); }

int8_t duckffi_value_i8(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int8(result, column, row); }
float duckffi_value_f32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_float(result, column, row); }
uint8_t duckffi_value_u8(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint8(result, column, row); }
double duckffi_value_f64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_double(result, column, row); }
int16_t duckffi_value_i16(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int16(result, column, row); }
int32_t duckffi_value_i32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int32(result, column, row); }
int64_t duckffi_value_i64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int64(result, column, row); }
uint16_t duckffi_value_u16(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint16(result, column, row); }
uint32_t duckffi_value_u32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint32(result, column, row); }
uint64_t duckffi_value_u64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint64(result, column, row); }
bool duckffi_value_boolean(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_boolean(result, column, row); }
bool duckffi_value_is_null(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_is_null(result, column, row); }
uint32_t duckffi_value_date(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_date(result, column, row).days; }
char* duckffi_value_string(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_varchar_internal(result, column, row); }
uint32_t duckffi_value_time(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_time(result, column, row).micros / 1000; }
uint32_t duckffi_value_interval_months(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_interval(result, column, row).months; }
uint64_t duckffi_value_timestamp_ms(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_timestamp(result, column, row).micros / 1000; }
uint32_t duckffi_value_interval_days(duckdb_result* result, uint32_t row, uint32_t column) { duckdb_interval interval = duckdb_value_interval(result, column, row); return interval.micros / 1000 + 24 * 60 * 60000 * interval.days; }

int8_t duckffi_value_i8_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int8(result, column, row); }
float duckffi_value_f32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_float(result, column, row); }
uint8_t duckffi_value_u8_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint8(result, column, row); }
double duckffi_value_f64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_double(result, column, row); }
int16_t duckffi_value_i16_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int16(result, column, row); }
int32_t duckffi_value_i32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int32(result, column, row); }
int64_t duckffi_value_i64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int64(result, column, row); }
uint16_t duckffi_value_u16_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint16(result, column, row); }
uint32_t duckffi_value_u32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint32(result, column, row); }
uint64_t duckffi_value_u64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint64(result, column, row); }
bool duckffi_value_boolean_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_boolean(result, column, row); }
bool duckffi_value_is_null_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_is_null(result, column, row); }
uint32_t duckffi_value_date_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_date(result, column, row).days; }
char* duckffi_value_string_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_varchar_internal(result, column, row); }
uint32_t duckffi_value_time_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_time(result, column, row).micros / 1000; }
uint32_t duckffi_value_interval_months_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_interval(result, column, row).months; }
uint64_t duckffi_value_timestamp_ms_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_timestamp(result, column, row).micros / 1000; }
uint32_t duckffi_value_interval_days_slow(duckdb_result* result, uint64_t row, uint32_t column) { duckdb_interval interval = duckdb_value_interval(result, column, row); return interval.micros / 1000 + 24 * 60 * 60000 * interval.days; }

duckdb_blob* duckffi_value_blob(duckdb_result* result, uint32_t row, uint32_t column) {
  duckdb_blob stack = duckdb_value_blob(result, column, row);
  duckdb_blob *blob = (duckdb_blob*)malloc(sizeof(duckdb_blob));

  memcpy(blob, &stack, sizeof(duckdb_blob));

  return blob;
}

duckdb_blob* duckffi_value_blob_slow(duckdb_result* result, uint64_t row, uint32_t column) {
  duckdb_blob stack = duckdb_value_blob(result, column, row);
  duckdb_blob *blob = (duckdb_blob*)malloc(sizeof(duckdb_blob));

  memcpy(blob, &stack, sizeof(duckdb_blob));

  return blob;
}

uint8_t* duckffi_null_bitmap(duckdb_result* result, uint32_t rows, uint32_t column) {
  uint8_t* bitmap = (uint8_t*)malloc(ceilf(rows / 8.0));

  for (uint32_t row = 0; row < rows; row++) {
    if (duckdb_value_is_null(result, column, row)) {
      bitmap[row / 8] |= (1 << (row % 8));
    }
  }

  return bitmap;
}