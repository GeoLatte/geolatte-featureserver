#!/bin/bash
#deployed enkel een nieuwe versie, bouwt dus geen docker, download die versie niet manueel, ...

package_name=$1
app_version=$2

ansible-playbook ./ansible/playbooks/redeploy.yml -i ./ansible/hosts -e "package_name=$package_name app_version=$app_version"
