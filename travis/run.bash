#!/bin/bash

set -e

py.test --cov=ckanext.importer ckanext/importer/tests

