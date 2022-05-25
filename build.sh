clang src/sql.c \
-O3 -flto \
-dynamiclib \
-mtune=native \
-o bin/libduckdb.dylib \
-undefined dynamic_lookup \
-lduckdb -L$(brew --prefix duckdb)/lib -I$(brew --prefix duckdb)/include