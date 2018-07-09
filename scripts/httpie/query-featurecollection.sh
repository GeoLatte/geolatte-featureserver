#!/usr/bin/env bash

if [[ -z "$1" || -z "$2"  ]]; then
    echo "$0 <db> <col> <query-string>"
    exit 1
fi

source $(dirname $0)/env.sh

#for Query use the https://httpie.org/doc#querystring-parameters
# example:
# ./query-featurecollection.sh featureserver adviezen EXPLODE==TRUE BBOX=='150256,209493,151992,210710' query=='intersects bbox'

DB=$1
CL=$2
shift
shift


http GET "$BASEURL/databases/$DB/$CL/query" "$@"
