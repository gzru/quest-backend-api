#!/usr/bin/env bash

set -e
set -x


if [ -f pre-install.py ]
then
    python pre-install.py
fi

exec "$@"

