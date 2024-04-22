# Workshop 002 ETL

This workshop focuses on extracting information from two different data sources a csv file and a database containing music information on Spotify and Grammys respectively. This data comes from kaggle and an ETL process will be built using Apache Airflow so that at the end a Dashboard is displayed from the data stored in the database to visualize the information in the best way.

## Technologies  used

- Python
- Jupyter Notebook 
- MySQL Relational Database
- Docker
- Airflow
- Power BI


## Steps for Use

1. Clone the repository on your local machine.

2. Installs the requirements.

3. In the terminal run the following command `docker-compose up airflow-init`

4. Then run `docker-compose up`

5. Log into: http://localhost:8080/

6. Run the dag: “workshop2_dags” 
