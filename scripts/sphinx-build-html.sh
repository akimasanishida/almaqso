#!/bin/bash

rm docs/diagrams/*.svg
plantuml --svg docs/diagrams/*.puml
rm -rf docs/_build/html
make -C docs html