#!/bin/bash
set -euo pipefail
shopt -s nullglob

DIAG_DIR="docs/diagrams"
C4_DIR="$DIAG_DIR/c4"

rm -f "$DIAG_DIR"/*.svg "$DIAG_DIR"/*.png

if [[ -n "${PLANTUML_JAR:-}" ]]; then
  java -jar "$PLANTUML_JAR" -tsvg -DRELATIVE_INCLUDE="$(pwd)/$C4_DIR" "$DIAG_DIR"/*.puml
else
  plantuml -tsvg -DRELATIVE_INCLUDE="$(pwd)/$C4_DIR" "$DIAG_DIR"/*.puml
fi

rm -rf docs/_build/html
make -C docs html