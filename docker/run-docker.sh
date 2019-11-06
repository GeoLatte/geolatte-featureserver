#!/usr/bin/env bash

docker run -d -p 9432:5432 --name gserver geolatte/featureserver-postgres:latest 