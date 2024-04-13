# data-center-scale-computing

For CSCI 5352: Datacenter-scale computing


Our goal is to build up a scalable data pipeline processing data from Austin Animal Shelter Outcomes.

Create a dockerized script reading data from a csv, processing it, and outputting into another csv

Create a dockerized postgres data warehouse to store the data

Use dimensional modeling for the data

Load the data into the DW through docker-compose

Change the pipeline to put the intermediate data into cloud storage at every step

Switch postgres DW to cloud DW

Orchestrate the pipeline with Airflow