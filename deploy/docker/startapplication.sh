#!/bin/sh

exec /ONT/geolate-nosqlfs/app/geolate-nosqlfs/bin/geolate-nosqlfs -J-Dconfig.file=/ONT/geolate-nosqlfs/app/geolate-nosqlfs/config/application.conf 2>&1
