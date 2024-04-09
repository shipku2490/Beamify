# Steps to run the code locally

* Install local Airflow (use dockerized setup) and PgAdmin
* Create a new database test in PgAdmin
* Create a table called "blockchain_data"
* Keep requirements.txt file, Dockefile inside the same folder where docker-compose file exists.
* Run the command "docker-compose up" in terminal
* Deploy the code in the DAGs folder in Airflow and turn on the DAG


# Steps to deploy the solution to the Google Cloud Platform

* The solution can be deloyed to the Cloud Composer (managed Airflow).
* To deploy enable the composer API in the cloud
* Create a standard composer cluster on GCP
* After the cluster is created it will create a DAG, logs, and plugins folder in Google Cloud Storage.
* copy the airflow code into the DAG folder, once the file is copied wait for few minutes for the DAG to harvest on the Airflow UI