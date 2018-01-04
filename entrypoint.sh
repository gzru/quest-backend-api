#!/usr/bin/env bash

set -e
set -x

python -m pre-install.aerospike_init
python -m pre-install.elasticsearch_init

exec "$@"

