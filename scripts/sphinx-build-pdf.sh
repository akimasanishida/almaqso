#!/bin/bash

uv run sphinx-build -b latex docs docs/_build/latex
cd docs/_build/latex
latexmk -norc -pdf -interaction=nonstopmode -halt-on-error *.tex
cp almaqso.pdf ../../