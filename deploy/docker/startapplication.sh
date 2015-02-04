#!/bin/sh

exec /ONT/locatieservices/app/locatieservices/bin/locatieservices -J-Dconfig.file=/ONT/locatieservices/app/locatieservices/config/application.conf 2>&1
