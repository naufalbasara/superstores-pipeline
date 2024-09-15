# End-to-end Dummy E-Commerce Data Pipeline
This project developed a data pipeline that deliver data from various data sources upstream (operational database) to data warehouse downstream. Database are modeled to have at least what businesses should have nowadays.

## Overview
Businesses nowadays are faced with various challenges. With a big amount of competitors, businesses compete to give the best solution to their customers. Technology becomes an enabler for business growth and being competitive. One of the requirements for business to be competitive is being data-driven. With data they can keep up to date so it can support decision making and deliver the right business decisions. 

In this project, data is being delivered end-to-end from database upstream (where the data is reserved) to data warehouse downstream in a data pipeline developed with Apache Airflow so it can be run according to schedule and dependant. To ensure data reliability, data integrity checks are required in a pipeline in order to deliver high quality data. Data stored in a data warehouse later can be utilized with business users to analyze the data, develop an operational dashboard, and develop machine learning models.

## Data Pipeline Overview
![E-commerce Data Pipeline](https://github.com/user-attachments/assets/0499bfb1-40ab-490e-977e-679d6c4be2a6)

## Data Structure

### Database Schema
![superstore-sales (Marketplace)](https://github.com/user-attachments/assets/248cd4c0-ee3a-4eb7-8a15-7f84a8e09d11)

This database schema created with the simplest data e-commerce business should have nowadays. Sales are separated in two table, the details of the orders and the details of the products being sold are separated in different tables.
Business should have recorded their customer who have trasacted in their platform. Customer records are useful for further analysis and customer profiling, analysis is used to see customer behavior and buying patterns so it can be useful for future business decision.

One more thing that important is table about customer visit in platform. Customer visit can be use to analyze the retention rate of each of the customer. Customer also can be grouped according to their locations, gender, and segmentation.

### Data Warehouse Modeling
![superstores-dw-schema](https://github.com/user-attachments/assets/2c1a8709-e338-4cdd-9a12-6e621c9b99fc)

Data warehouse model in this project use a star schema containing two fact tables and dimension tables. Fact tables contains quantitative data that need to be measure, e.g. sales, marketing metrics, etc.
While dimension tables used to provide context to data in fact tables.

## DAGs in detail

DAGs is a sequence of workflow that runs in a schedules interval time. In this project DAGs are divided into a few small chunks of sequential work and have a different purpose of work. 
The first DAG is used as an extraction from operational DB, and done the transformation along the way. Fetched and transformed data then are saved in a local storage.

The second and third DAG is use to load the stored data after transformation earlier to database staging. In the staging environment, data are prepared before it gets to the data warehouse. 
Data integrity checks comes into help to maintain data reliability.

Data integrity checks ensure the data is ready to be consumed and reliable to business users. The checks including the checking duplication of data, checking unique constraints, missing values, right formatting, and completeness to identify potential issues.

### DB Extraction & Transformation DAG
<img width="1258" alt="image" src="https://github.com/user-attachments/assets/7514f38f-2730-483b-b3fa-39442f7f0db4">

### Load Dimension Tables to Staging DB DAG
<img width="684" alt="image" src="https://github.com/user-attachments/assets/d20cc2e5-5235-4d66-82e8-05b36cf3d88a">

### Load Fact Tables to Staging DB DAG
<img width="656" alt="image" src="https://github.com/user-attachments/assets/3b384cf0-6295-4f35-9844-2aff3740ce19">



**Stacks**: Airflow for Data Orchestration, Postgres for DB, Python for ETL

**NOTE**: Data in this project are took from [Kaggle](https://www.kaggle.com/datasets/bravehart101/sample-supermarket-dataset) where the data has been duplicated and engineered in a business simulation so that it seems like a running business.




