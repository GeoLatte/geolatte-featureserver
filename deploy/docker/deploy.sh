#!/bin/bash

package_name=$1
app_version=$2

ansible-playbook ./ansible/playbooks/full_deploy.yml -i ./ansible/hosts -vvv -e "package_name=$package_name app_version=$app_version"
