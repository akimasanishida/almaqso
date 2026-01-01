#!/bin/bash

rm docs/diagrams/*.svg
plantuml --svg docs/diagrams/*.puml
rm -rf docs/_build/latex
make -C docs latex
cd docs/_build/latex
latexmk -norc -pdf -interaction=nonstopmode -halt-on-error *.tex
cp manual.pdf ../../