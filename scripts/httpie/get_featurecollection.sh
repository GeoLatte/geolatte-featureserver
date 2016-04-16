#!/usr/bin/env bash

if [[ -z "$1" || -z "$2" || -z "$3" ]]; then
    echo "$0 <db> <col> <num>"
    exit 1
fi

source $(dirname $0)/env.sh

http GET "$BASEURL/databases/$1/$2/featurecollection?limit=$3"