#!/bin/bash
# Generate doxygen locally for testing.

source ./defineVariables.sh

set -e
set -v

cd ../web
rm -rf doxygen/html
doxygen fennel.doxycfg
