#!/usr/bin/env bash

if [[ -z "$1" || -z "$2"  ]]; then
    echo "$0 <db> <col> <query-string>"
    exit 1
fi

QUERY=$3
if [[ -z "$QUERY" ]]; then
    QUERY="limit=1"
fi

source $(dirname $0)/env.sh
echo "GET: $BASEURL/databases/$1/$2/featurecollection?${QUERY}"
http GET "$BASEURL/databases/$1/$2/featurecollection?${QUERY}"
