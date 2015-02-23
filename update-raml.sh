git checkout master -- raml
raml2html raml/api.raml > service-catalog.html
git add service-catalog.html
git commit -m "Updates raml."
