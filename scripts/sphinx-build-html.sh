#!/bin/bash

plantuml --svg docs/diagrams/*.puml
make -C docs html