#!/bin/bash
set -euo pipefail
shopt -s nullglob

DIAG_DIR="docs/diagrams"
C4_DIR="$DIAG_DIR/c4"

rm -f "$DIAG_DIR"/*.svg
plantuml -tsvg -DRELATIVE_INCLUDE="$C4_DIR" "$DIAG_DIR"/*.puml

rm -rf docs/_build/html
make -C docs html