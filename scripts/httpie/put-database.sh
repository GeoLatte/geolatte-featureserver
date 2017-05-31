#!/usr/bin/env bash

if [[ -z "$1"  ]]; then
    echo "$0 <db> "
    exit 1
fi

source $(dirname $0)/env.sh

http PUT $BASEURL/databases/$1

