#!/bin/bash
#
# Installeer een gegeven artifact naar een bepaalde machine.
#
# De user 'ansible' moet bestaan op de machine en moet sudo rechten hebben.
# Om de 'ansible' user alle nodige rechten te geven kun je als root volgende stappen uitvoeren op de doelmachine:
# > chmod o+w /etc/sudoers
# > vi /etc/sudoers
# Voeg volgende 2 regels toe aan /etc/sudoers:
# | # Give ansible sudo rights without asking a password:
# | ansible ALL=(ALL) NOPASSWD:ALL
# > chmod o-w /etc/sudoers
#

if test "$#" -lt 2; then
    echo "Geef het path op naar het artifact en de host waarop die geïnstalleerd moet worden."
    echo "Bijvoorbeeld: deployAwvArtifact.sh /path/to/wdb-web-3.3.0.0-SNAPSHOT.war dev"
    exit 1
fi

pathToArtifact=$1
targetHost=$2

artifact=`echo $pathToArtifact | sed 's,^[^ ]*/,,'`
package_ext=`echo $artifact | sed 's/.*\.//'`

if [[ $artifact == *SNAPSHOT* ]]
then
  app_version=`echo $artifact | sed 's/[^0-9.]*\([0-9.]*-SNAPSHOT\).*/\1/'`
  package_name=`echo $artifact | sed 's/\([^0-9.]*\)-[0-9.]*-SNAPSHOT.*/\1/'`
else
  app_version=`echo $artifact | sed 's/[^0-9.]*\([0-9.]*[0-9]\).*/\1/'`
  package_name=`echo $artifact | sed 's/\([^0-9.]*\)-[0-9.]*.*/\1/'`
fi

echo "Installeren van $package_name versie $app_version op $targetHost uit een $package_ext artifact."

rm -rf /tmp/deploy/$package_name
mkdir -p /tmp/deploy/$package_name
cp $pathToArtifact /tmp/deploy/$package_name
cd /tmp/deploy/$package_name

# Ga na of de deploy directory op het root niveau van de zip file zit, of dieper (onder de $artifact directory).
# In een war file zal die typisch op het root niveau zitten, terwij in een zip file (gemaakt via de SBT universal packager)
# zal die nog onder de directory zitten met de naam van het artifact (e.g. 'locatieservices-2.1.0.0-SNAPSHOT/deploy/...').
if unzip -l $pathToArtifact | grep ' deploy/' ;
then
  deploy_filter="deploy/*" 		# op het root niveau
else
  depoy_filter="*/deploy/*"		# dieper in de zip file
fi

unzip $pathToArtifact $deploy_filter
mkdir $package_name
mv `find ./ -name "deploy" -type d | head -1` $package_name
cd $package_name/deploy/ansible

# Hier kunnen we bijkomende host vars toevoegen en dan in de hosts file 'inventory=somemachine' toevoegen.
# De optie -K moet meegegeven worden als de ansible user verwacht wordt een passwoord op te geven bij het sudo commando.
# Dit is het geval als er in /etc/sudoers 'ansible ALL=(ALL) ALL' werd geconfigureerd i.p.v. 'ansible ALL=(ALL) NOPASSWD:ALL'.
ansible-playbook ./playbooks/app.yml -i ./${targetHost}_hosts -e "package_name=$package_name app_version=$app_version baseline_path=/tmp/deploy/$package_name" -v -vvv
