#! /bin/bash


if [[ -z "$1" || -z "$2" ]]; then
    echo "$0 <db> <col>" 
    exit 1
fi

source $(dirname $0)/env.sh

http POST $BASEURL/databases/$1/$2/tx/insert

