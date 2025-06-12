#!/bin/bash

set -e

# Run the tests
py.test --cov=ckanext.importer ckanext/importer/tests

# Build the documentation
./make_docs.sh

# Check that CHANGELOG.md contains valid Markdown
python -m markdown CHANGELOG.md

# Check that README.rst contains valid reStructuredText
rst2html.py README.rst

