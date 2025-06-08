# Project part 1 - Landing Zone

## Cloning the repository

```bash
git clone https://github.com/nhungoc1508/S25-BDM-Project.git
```

## Setting up the data simulation container
```bash
cd S25-BDM-Project
cd data-simulation
docker network create data-processing-network
docker compose up -d
```

Check that the PostgreSQL server is running and data has been loaded:
```bash
docker logs postgres_sis | grep "Mock data"
```
Should see either `Mock data inserted successfully!` or `Mock data already exists. Skipping insertion.`

Check that the MongoDB server is running and data has been loaded:
```bash
docker logs mongo_counselors | grep "Mock data"
```
Should see `Mock data inserted successfully!`

## Setting up Delta Lake and the ingestion pipeline

> [!IMPORTANT]
> If you are running locally, **comment out** this line in the `docker-compose.yaml` file (this line appears 3 times under `spark-master`, `spark-worker-1`, and `spark-worker-2`):
> ```bash
> platform: linux/arm64
> ```

### Setting up Spark master and workers
Build custom images for Spark and Airflow:
```bash
cd ../delta-lake
sudo docker build -t custom-airflow -f Dockerfile.airflow .
sudo docker build -t custom-spark -f Dockerfile.spark .
```
Start MongoDB and Neo4J:
```bash
docker compose up counseling-db graph-db -d
```
Start master:
```bash
docker compose up spark-master -d
```
Check that master is running:
```bash
docker logs spark-master | grep "I have been elected leader! New state: ALIVE"
```
Start workers:
```bash
docker compose up spark-worker-1 spark-worker-2 -d
```
Check that all nodes are running and the workers are registered with master:
```bash
docker logs spark-worker-1 | grep "Successfully registered with master spark://spark-master:7077"
docker logs spark-worker-2 | grep "Successfully registered with master spark://spark-master:7077"
```
In case of failure to register, run `compose down` then repeat previous steps:
```bash
docker compose down spark-master spark-worker-1 spark-worker-2 -v
```
Once running, the Spark master UI is available at `localhost:8081/` and will show 2 alive workers:
<center><img src="imgs/spark-ui.png" width=500/></center>

### Setting up Airflow
```bash
mkdir -p ./data ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
```
Check that Airflow is using PostgreSQL for metadata (and not SQLite):
```bash
docker logs airflow-init | grep "DB: postgresql+psycopg2"
```
Start the rest of the Airflow-related services:
<!-- docker compose up -d -->
```bash
docker compose up airflow-worker airflow-scheduler airflow-dag-processor airflow-apiserver airflow-triggerer airflow-cli flower -d
```
Check that the webserver UI is up and running:
```bash
docker logs airflow-apiserver | grep "Application startup complete"
```
The Airflow webserver is available at `localhost:8080/`:
<center><img src="imgs/airflow-ui-login.png" width=800/></center>

Log in with username `airflow` and password `airflow`. After logging in, click on the **Dags** tab on the left menu bar, the webserver UI will list all available DAGs:

<center><img src="imgs/airflow-ui-main-2.png" width=800/></center>

## Running DAGs to submit Spark ingestion jobs

### Setting up connection to Spark master

In the Airflow webserver UI, go to **Admin** > **Connections**. Select **Add Connection** and add a connection to the Spark master with ID `spark-default`, type `Spark`, host `spark://spark-master`, and port `7077`:

<center><img src="imgs/airflow-connection-2.png" width=600/></center>
![alt text](image.png)

### Running DAGs
Either trigger the DAGs manually or wait for scheduled runs, and monitor the DAG logs:
<center><img src="imgs/airflow-ui-run-2.png" width=600/></center>
