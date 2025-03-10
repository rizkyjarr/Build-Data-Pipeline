# Biodiesel Company's Analytics | Building data pipeline using BigQuery, Airflow, and Docker


## Abstract

- This project was completed by Rizky Fajar Aditya as part of the learning materials at Purwadhika School. 
- This project objective is to build a batching data pipeline
- Key Technologies like Airflow, PostgreSQL, Docker, BigQuery, and Google Looker are utilized in this project.
- Project is successfully done

## Task Description

- Every data engineering student at Purwadhika School is tasked to create a data-pipeline using key technologies, such as: PostgreSQL, Airflow, and BigQuery.
- Student can choose their own study case. In my case: i chose to create a data-pipeline that can serve information and business insights for Biodiesel Company and create dashboard using Google Looker.

## Project Process

I applied Software Development Cycle (SDLC) on this project to keep the project systematical and methodical, the stages are:
1. Gathering Requirement from key stakeholders
2. Designing data pipeline architecture <img src='data_architecture_design.png' alt='data architecture design' width='100%'>
3. Code Development:
    - Build PostgreSQL using Docker: [Reference code here](https://github.com/rizkyjarr/Build-Data-Pipeline/blob/main/postgresql_app/docker-compose.yaml)
    - Build Airflow using Docker: [Reference code here](https://github.com/rizkyjarr/Build-Data-Pipeline/blob/main/airflow_app/docker-compose.yaml)
    - Build 1st DAG Script (generate transactional data and ingest to PostgreSQL): [Reference code here](https://github.com/rizkyjarr/Build-Data-Pipeline/blob/main/airflow_app/dags/DAG1_load_to_postgre.py)
    - Build 2nd DAG Script (fetch data from postgreSQL and incremental load to BigQuery): [Reference code here](https://github.com/rizkyjarr/Build-Data-Pipeline/blob/main/airflow_app/dags/DAG2V2_load_to_BQ.py)
4. Testing
5. Deployment
6. Maintenance

The whole documentation can be read [here](https://github.com/rizkyjarr/Build-Data-Pipeline/blob/main/Capstone3_RizkyFajarAditya.pdf)    

## Results

All code was successfully debugged and worked to perform the task based on the data pipeline architecture design above.

<img src='project_results.png' alt='project results' width='100%'>

