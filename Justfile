run: build
    build/MyMain

bench: build
    hyperfine build/MyMain

triangles:
    duckdb < triangle.sql

build:
    cmake --build build --config Release
