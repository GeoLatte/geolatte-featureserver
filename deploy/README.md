# Overzicht

De deploy directory bevat twee subdirectories: ansible en docker. De ansible directory bevat de 
basisconfiguratie voor ansible, de docker directory bevat een bijkomende ansible configuratie 
om een docker voor dit project af te halen en deze in een vagrant image te deployen. In deze
docker wordt dan vervolgens de applicatie geïnstalleerd a.d.h.v. de ansible basisconfiguratie. 

De naam en de structuur van de deploy directory ligt vast zodat deze als een conventie bruikbaar 
wordt voor externe scripts. Deze directory wordt mee in het artifact gepackaged zodat het steeds 
aanwezig is als men een artifact wil deployen op een gegeven systeem. 

## Ansible basisconfiguratie

De main entry point voor ansible is het bestand 'app.yml'. De naam van dit bestand ligt vast zodat 
het aangesproken kan worden vanuit externe scripts, zoals het 'deployAwvArtifact.sh' script. Vanuit 
dit yml bestand kunnen dan uiteraard andere yml bestanden en templates aangeroepen worden. Er zijn 
ook enkele host bestanden en bestanden met host variabelen aanwezig voor enkele courante systeemen. 
Uiteraard kunnen deze aangevuld worden door de externe scripts die van de ansible playbook gebruik 
maken.

## Docker ansible configuratie

