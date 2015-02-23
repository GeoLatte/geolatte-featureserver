This is a FeatureServer for NoSQL databases.

A Feature Server provides a REST API for querying and managing (geospatial) data. Currently, the NoSQL-FeatureServer supports MongoDB and Postgresql 9.3 only.

The REST API is documented in the service catalog, published [here](http://geolatte.github.io/geolatte-nosql/service-catalog.html).

============================
Building NoSQL-FeatureServer
============================

* install sbt 0.11.2  if you do not have it already. You can get it from here: https://github.com/harrah/xsbt/wiki/Getting-Started-Setup

* execute 'sbt' and then `help play` for play specific commands

* execute `sbt` and then `compile` to build the project

* execute `sbt` and then 'run' to run the built-in development server


=============================
Postgresql note's
=============================

Prerequisites: create the database that will hold the schema's and tables managed by this server. For best performance of regex expression, make sure
the pg_trgm extension is enable like so:

    psql=# CREATE EXTENSION pg_trgm;






