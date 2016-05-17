#! /bin/bash


if [[ -z "$1" || -z "$2" ]]; then
    echo "$0 <db> <col>"
    exit 1
fi

source $(dirname $0)/env.sh

http PUT $BASEURL/databases/$1/$2 <<EOF
{
    "index-level" : 8,
    "id-type" : "text",
    "extent" : {
        "crs" : 31370,
        "envelope" : [0,0,300000, 300000]

    }
}
EOF

