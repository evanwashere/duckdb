clang src/sql.c \
-O3 -flto \
-dynamiclib \
-mtune=native \
-o bin/libduckdb.dylib \
-undefined dynamic_lookup \
-lduckdb -L/opt/homebrew/opt/duckdb/lib -I/opt/homebrew/opt/duckdb/include