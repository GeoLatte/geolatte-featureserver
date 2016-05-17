#!/usr/bin/env bash

#! /bin/bash


if [[ -z "$1" || -z "$2" ]]; then
    echo "$0 <db> <col>"
    exit 1
fi

source $(dirname $0)/env.sh

http POST $BASEURL/databases/$1/register <<EOF
{
    "extent" : {
        "crs" : 31370,
        "envelope" : [0,0,300000, 300000]

    },
    "collection" : "$2",
    "geometry-column": "geometry"
}
EOF
