# Data Engineering Zoomcamp Week 2: Workflow Orchestration (with Prefect)
- For taking notes for my Data Engineering Zoomcamp &amp; share code as part of my homework



# Data Lake (GCS)

## What is a Data Lake


แหล่งรวบรวมข้อมูลในทุกรูปแบบ (จับฉ่าย) 

- It is a repository that collect all forms of data disregarding structured or non-structured.  

- The main idea is to store data as much as possible, as quick as possible, and can be acces to data fastly for team members.


## ELT vs. ETL

- Export Transform and Load (ELT) is for small amount of data = **Data Warehouse solution**

- Export Load and Transform (ETL) is for large amount of data = **Data Lake solution**

-   [Video](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
-   [Slides](https://docs.google.com/presentation/d/1RkH-YhBz2apIjYZAxUz2Uks4Pt51-fVWVN9CcH9ckyY/edit?usp=sharing)

# [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#1-introduction-to-workflow-orchestration)1. Introduction to Workflow orchestration

## What is orchestration?

- Data orchestration is the process of taking siloed data from multiple data storage locations, combining and organizing it, and making it available for data analysis tools. And it makes data accessible across technology systems.


🎥 [Video](https://www.youtube.com/watch?v=8oLs6pzHp68&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=16)

# [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#2-introduction-to-prefect-concepts)2. Introduction to Prefect concepts

## What is Prefect?

- Prefect is the open source dataflow automation platform that will allow us to add observability and orchestration by utilizing python to write code as workflows to build,run and monitor pipelines at scale.


## Create Conda Environment

- To create conda environment, run

```
conda create -n zoom python=3.9
```

- Prompt will ask you to proceed, enter yes
- It till start download & extract packages on your environment

- To activate this environment, use ```conda activate zoom``` 

- To deactivate an active environment, use ```conda deactivate```

### Activate Your Conda Environment 

- to work on your project, activate 'zoom' environment ```conda activate zoom```  

- Now you're in "zoom" environment, then 

## Preparing Environment

- prepare ```requirements.txt``` file in your working directory
- 
- This ```requirements.txt``` contains tools you need to work on this project:

		``` 
		
		pandas==1.5.2
		
		prefect==2.7.7
		
		prefect-sqlalchemy==0.2.2
		
		prefect-gcp[cloud_storage]==0.2.4
		
		protobuf==4.21.11
		
		pyarrow==10.0.1
		
		pandas-gbq==0.18.1
		
		psycopg2-binary==2.9.5
		
		sqlalchemy==1.4.46
		
		``` 

- run

```
pip install -r requirement.txt
``` 

- Note: if you have already installed all other tools, you might just need to install Prefect.

- To install Prefect, run  
```
pip install prefect -U
``` 

After preparing environment, we'll start digest data

## Digest Data

- create file ---> ```ingest_data.py``` in your working directory Or download a script here ---> https://github.com/discdiver/prefect-zoomcamp


- Adjust parameters to fit your environment, especially in the "**main**" function area. All parameter must be matched to your network/host environment.


- Then **make sure you're put up your network & connected to host server, and have database created**.


- Then in "zoom" enviroment (or your conda environment), run

```
python ingest_data.py
```

- Now, you must have data table created in your database already! 


	- You can quickly check through pgcli environment, 


		- type ```\dt``` <-- you'll see that the table's created


		- type ```SELECT count(1) FROM xx[your table name]xx``` <-- to check if all data is ingested in the table


	- Or go check on pgAdmin localhost:8080, on server > host name > database > schema > table > view your data

- Next we will create Prefect Flow

## Create Flow with Python
Create ```ingest_data_flow.py``` 


```
#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
  

@task(log_prints=True, tags=["extract"])
def extract_data(url: str):
	# the backup files are gzipped, and it's important to keep the correct extension
	# for pandas to be able to open the file
	if url.endswith('.csv.gz'):
	csv_name = 'yellow_tripdata_2021-01.csv.gz'
	else:
	csv_name = 'output.csv'
	os.system(f"wget {url} -O {csv_name}")


	df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

  
	df = next(df_iter)

	df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

	df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

	return df

  

# next task is to remove passenger that = 0

@task(log_prints=True)
def transform_data(data):
	print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
	df = df[df['passenger_count'] != 0]
	print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
	return df

  

@task(log_prints=True, retries=3)
def load_data(user, password, host, port, db, table_name, df):
	postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
	engine = create_engine(postgres_url)

	df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

	df.to_sql(name=table_name, con=engine, if_exists='append')

  

# A flow can also call another flow (= add subflow into the flow)
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
	print(f"Loggin Subflow for: {table_name}")

  

@flow(name="Ingest Data")
def main_flow(table_name: str = "yellow_taxi_trips"):
	user = "xxx"
	password = "xxx"
	host = "xxx"
	port = "xxx"
	db = "ny_taxi"
	csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
	
	log_subflow(table_name)
	raw_data = extract_data(csv_url)
	data = transform_data(raw_data)
	load_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
	main_flow(table_name = "yellow_trips")

```

- Adjust detail to fit your environment

- Then run 

```
python ingest_data_flow.py
```

- Next, to see your flow runs 👇

## Visualize Your Flow Runs

- Open another Terminal

```
conda activate zoom
```

```
prefect orion start
```

- You're now on orion server 


- On the console, you'll see "prefect config set PREFECT_API_URL=http://...." with server 


- Copy it and run it on another Terminal (in conda environment)


- Now visit the dashboard on Prefect server by clicking the link on Prefect orion or past the Prefect APIs Url on the console to your browser. 

- You can now see all the flow runs.

- Next we will learn to create Prefect Block

## Create Prefect Block

### What is Prefect Blocks & Why do we need them?

- Block will keep your code, config that you have created and you can reuse them anytime

- On orion server, click Block select **SQLAlchemy Connector**

- Click add, setup 

	Block name: postgres-connector

	Driver: postgresql+psycopg2

	Database: ny_taxi

	Username: postgres

	Host: localhost

	Port: 5433 (port that you use)

	Click create


- Paste this script to your ingest_data_flow.py file 👇


```
from prefect_sqlalchemy import SqlAlchemyConnector

with SqlAlchemyConnector.load("postgres-connector") as database_block:
```


🎥 [Video](https://www.youtube.com/watch?v=jAwRCyGLKOY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=17)

## [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#3-etl-with-gcp--prefect)3. ETL with GCP & Prefect

# Export Load and Transform (ETL) with GCP & Prefect

## Flow 1: Putting data to Google Cloud Storage

🎥 [Video](https://www.youtube.com/watch?v=W-rMz_2GwqQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=18)

## Preparing GCP
- Setup your gcloud account https://cloud.google.com/


- Create project, select project


- Go to Cloud Storage > Buckets, setup your Bucket "prefect_de_zoom"


- Next, go to IAM & Admin to create service account


- Setup Service Account, set role as BigQuery Admin, Storage Admin > done


- Select you Service Account > select keys on the top menu bar > clicke key > create new key > select JSON > create


- Download that file into your safe folder. (We'll need it to setup Block later on)


- Next we will transform csv to parquet and upload it to gcloud

## Setup Environment for Prefect Flow
- Make sure you're in your environment 

```
conda activate [your environment name]
```

- Activate Prefect orion server, so you can observe what's going on while working on data using Prefect flow runs

```
prefect orion start
```

- Then we will create Gcloud Block on Prefect


	- On Terminal (zoom environment), call  ```prefect block register -m prefect_gcp```


	- On prefect orion dashboard, go to Block > Add+ GCS Bucket, then setup


		- Block name: zoom-gcs


		- Bucket name: prefect_de_zoom xx(should be matched with your gcloud bucket)xx


		- GCP Credentials > add > 
			- Block name: zoom-gcs-creds 
			- Service Account info: place the code in your JSON file that you've just downloaded from setting up GCP Service Account > click create
		- Then select credentials > click create
		- You'll get code
		- Copy them to write your script in the next step

### Create Python file for Prefect Flow Runs

	- Open the Prefect orion dashboard
	
	
	- Create a python file ```elt_web_to_gcs.py``` which create flow runs to:
	
		- Read csv file & create data frame
		
		- Clean/manipulate data
		
		- Write the data frame locally to a parquet file
		
		- Upload the local parquet file to gcloud



``` 
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()

``` 



	- In your conda environment, run ```python elt_web_to_gcs.py``` 

	- Now go check gcloud, on your Cloud Storage > Buckets, you should see that the data (parquet file) is now inside your Bucket 
	
	- Also on Prefect orion dashboard, you will see flow runs log, and Block you've created
	



## [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#4-from-google-cloud-storage-to-big-query)4. From Google Cloud Storage to Big Query

## Flow 2: From GCS to BigQuery

🎥 [Video](https://www.youtube.com/watch?v=Cx5jt-V5sgE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19)

- On gcloud, go to BigQuery on the left menu bar > +ADD DATA > Google Cloud Storage > select the file from GCS Bucket 


	- File format: Parquet
	- Project name: xxx
	- Dataset: yellow_taxi_prefect/green_taxi
	- Table: yellow_taxi_prefect_rides/green_taxi_prefect_rides
	- Create table
- After setting Table on GCS, you can now create script for Prefect flow runs "etl_gcs_to_bq.py"



``` 
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="prefect-sbx-community-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()

``` 




- This flow runs will do:
	- Extract data from GCS to your local (parquet file)
	- Read & Transform data, create Data Frame
	- Write Data Frame to gcloud
- After execute the ```etl_gcs_to_bq.py``` 
	- Go check the table on gcloud, use query tool to acquire data

## [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#5-parametrizing-flow--deployments)5. Parametrizing Flow & Deployments


## Add Parameterization to Prefect Flow

### Why Parameterize?
- Parameterization allow your flow to take parameters, so you can have multiple flow runs

### How?
- First thing is to put parameter in the main flow. (In this course, ---> etl_web_to_gcs)

```
@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
	"""The main ETL function"""
	dataset_file = f"{color}_tripdata_{year}-{month:02}"
	dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

	df = fetch(dataset_url)
	df_clean = clean(df)
	path = write_local(df_clean, color, dataset_file)
	write_gcs(path)
```

- Next, create parent flow. Since we want to run multiple flows with different parameters at one flow run (at once).


- So we create parent flow, and pass on parameters months, year, color


- After running the prefect flow, this parent flow will trigger the "etl_web_to_gcs" flow and create subflow with different parameters (e.g. months) when you call. 

```
@flow()
def etl_parent_flow(
	months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
	):

	for month in months:
		etl_web_to_gcs(year, month, color)
```


## Create Deployment

### Why Deployment?

- Deployment inside Prefect allow you to schedule flow runs and **automatically** trigger flow runs


- It encapsulates the flows, schedule and trigger them **via API**


- You can also have multiple deployments for a single flow

### How?
First, **Build a Deployment**

- There are 2 main ways to build a deployment
	- Build through CLI
	- Build with Python

#### Build a Deployment with CLI

- Go to Terminal (zoom environment)


- Run, ```prefect deployment build /path/to/file.extension:entry-point-to-the-flow -n "name of the deployment"```

```
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```

- You'll see the yaml file ---> ```etl_parent_flow-deployment.yaml``` appears in your directory


- Open the file & add some parameters that you want to run

### Deployment Apply
- **prefect deployment apply** will comunicate with Prefect API


- On Terminal (zoom environment), run


```
prefect deployment apply etl_parent_flow-deployment.yaml
```

- On Prefect dashboard, go to Deployments > Parameters > on the right corner, select Quick Run


- > Work Queues, you'll see a flow run's waiting but not run unless we create agent

### Create Agent
- On Terminal, run


```
prefect agent start --work-queue "default"
```

- Now the flow is running

#### Create a Notification

- When you have automatically flow runs, you want to know what's going on with the flow runs.


- You can set the notification as well


- Go to Notifications >


	- Run states: select a state you want to get notice
	- Tags: add your tag
	- etc.
	- > Create

🎥 [Video](https://www.youtube.com/watch?v=QrDxPjX10iw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=20)


## [](https://github.com/suphawadeeth/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration#6-schedules--docker-storage-with-infrastructure)6. Schedules & Docker Storage with Infrastructure

- In the last session, we learned that Deployment in Prefect allow you to schedule your flow runs via APIs and we create a deployment with CLI.

- This session we will create and schedule a deployment with python through Docker contrainer 

## Create Deployment with Python

First, we want to store our flow code on a hub (gcloud, GitHub, Docker Hub etc.), where people can access the code and work with it. 

In this session we will store our flow code using Docker Image

### Create Docker Image & Store Flow Runs on Docker Hub

- Create a Dockerfile in your directory, which will do following tasks:

	- Start FROM Prefect base image
	
	- COPY ```docker-requirements.txt``` to prepare environment for this flow runs, this will install those tools in the ```docker-requirements.txt```


	- COPY flow code into the Docker Image, (note: you can copy a file or a whole folder)


Dockerfile script : 

```
FROM prefecthq/prefect:2.7.7-python 3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows

COPY data /opt/prefect/data
```

- On Terminal, run


```
docker image build -t <your-docker-hub-username>/prefect:zoom .
```

- Push the Image


```
docker image push <your-docker-hub-username>/prefect:zoom
```


### Create Prefect Block for Docker

- Go to Prefect dashboard


- Block > Docker Container


	- name: zoom
	- image: <your-docker-hub-username>/prefect.zoom
	- imagePullPolicy: Always
	- Auto Remove: on
	- > Create & copy that code, we will use in the next step 👇

### Write a Python file to Create a Deployment
	
- create python file ---> **docker_deploy.py**

```
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()


```

- On Terminal, run

```
docker_deploy.py
```

- Go to Prefect dashboard > Deployment
	
- Now you should see the flow name "docker-flow"
	
- You can also check prefect profile on Terminal ```prefect profile ls```
	
- It shows 'default' profile, mean we're using local API
	
- We can specify API for a specific URL that we want to use, run
	
```
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```  

### Schedule a Deployment using Prefect UI

On Prefect dashboard, Deployment tab
	
- Click on the deployment > add Schedule > set the schedule you want (interval, Cron, RRule)
	
- Or you can schedule deployment while building it using command line 👇

#### Create & Schedule a Deployment using  Command Line
- Let's try schedule the deployment (which you've already created) here name "etl"
	
- Specifiy which type of schedule. Here we use cron type 👇 , run it on Terminal

```
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL" --cron "0 0 * * *" -a
```

#### Create Agent to Trigger The Deployment

After we have scheduled the deployment, we want to create Agent to pick it up & trigger the deployment

```
prefect agent start -q default 
```

- Now Agent is created.
- You can now run the flow, run

```
prefect deployment run <name of the deployment> -p "months=[1,2]"
```

- ```-p``` This -p tag is for overiding the parameters, so you can change parameters here


🎥 [Video](https://www.youtube.com/watch?v=psNSzqTsi-s&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=21)




