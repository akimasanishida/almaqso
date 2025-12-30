#!/bin/bash

make -C docs latex
cd docs/_build/latex
latexmk -norc -pdf -interaction=nonstopmode -halt-on-error *.tex
cp almaqso.pdf ../../