FeatureServer provides a REST API for reading and writing spatial features from a Postgresql database.

The REST API is documented in the service catalog, published [here](http://geolatte.github.io/geolatte-featureserver/service-catalog.html).

Currently only Postgresql 9.3 and higher are supported.

Prerequisites: create the database that will hold the schema's and tables managed by this server. For best performance of regex expression, make sure
the pg_trgm extension is enable like so:

    psql=# CREATE EXTENSION pg_trgm;






