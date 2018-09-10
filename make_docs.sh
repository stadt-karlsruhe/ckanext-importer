#!/bin/sh

# -n: nit-picky mode, warn about missing references
# -W: turn warnings into errors
sphinx-build -b html \
             -n \
             -W \
             docs \
             docs/_build

