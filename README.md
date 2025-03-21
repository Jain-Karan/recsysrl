# Optimising Recommendation Systems

![image](./images/Optimising%20Recommendation%20Systems.png)

## RL Agents and Environment  

The python notebooks have all the relevant code. To run the simulation we need to g this code.

## Data Pipeline  

### Create Network

We need to setup a internal network for the pipeline to interact within the Docker stack.  

To do that we run the following command : 

> `docker network create cassandra-kafka`

### Compose Stack  

To run the docker stack we need to bring up the Airflow services and then build the container upon the Dockerfile to start all other services.  

To do that we run the following commands in the root directory :  

> `docker compose up airflow-init`
> `docker compose up -d --build`

#### Hindrances

> This is primarily a Windows issue as the permissions are not granted to the containers and services as expected. 

We need to check if the following folders have not been created : 

 - logs/dag_processor_manager
 - logs/scheduler

If not then there is usually a permission issue as we have mounted this volume and between Docker and we need to provide permission to Airflow to be able to create the folders.

To do that run the following command

> `docker exec airflow-worker chmod 777 logs && chmod 777 dags`

### Kafka Topics  

The purpose of the primary flow on the Airflow DAG is to create the Kafka topics if it is to be updated.
