#!/usr/bin/env bash

if [[ -z "$1" || -z "$2"  ]]; then
    echo "$0 <db> <col> <query-string>"
    exit 1
fi

if [[ -z "$3" ]]; then
    3="limit=1"
fi

source $(dirname $0)/env.sh

http GET "$BASEURL/databases/$1/$2/query?$3"
