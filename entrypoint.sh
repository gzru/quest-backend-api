#!/usr/bin/env bash

set -e
set -x

python -m pre-install.aerospike_initialization

exec "$@"

