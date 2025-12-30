#!/bin/bash

plantuml --svg docs/diagrams/*.puml
make -C docs latex
cd docs/_build/latex
latexmk -norc -pdf -interaction=nonstopmode -halt-on-error *.tex
cp almaqso.pdf ../../