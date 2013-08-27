#! /bin/bash
if [[ -z "$1" || -z "$2" ]]
then
	echo "Usage: $0 <dbname> <collectionname>"
else
	ogr2ogr -nln $2 -f "PostgreSQL" PG:dbname=$1 "http://localhost:9000/api/databases/$1/$2/download" OGRGeoJSON 	
fi

