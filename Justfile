container-shell:
    apptainer exec container.sif bash

container:
    apptainer build --force container.sif container.def

run: build
    build/MyMain example-spec.json | jq .

bench: build
    hyperfine build/MyMain

triangles:
    duckdb < triangle.sql

build:
    cmake --build build --config Release
