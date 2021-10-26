# Data Modeling with Postgres at Sparkify

## Introduction

The data engineering team at Sparkify, the music streaming app has a new project request from the data science & analytics team. 
In order to better understand user behavior and build more accurate recommender systems, the analytics team has approached the data engineering team to model data for their new use case and to set up an end-to-end ETL process so that they can use the final tables to perform faster analytics and build reliable recommender systems. 
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. Therefore, the analytics team wants this user activity data to reside in a Postgres database with tables designed to optimize queries on songplay analysis. As the lead data engineer for this project, my role is to create a database schema and an ETL pipeline for this analysis.   

## Datasets

### Song Dataset

The song dataset contains information like `artist_id`,`artist_name`,`artist_location`,`song_id`,`title`,`duration`, etc.

### Logs Dataset

The logs dataset contains app data that gives us information about what the users are listening. Information such as `artist`,`auth`,`firstName`,`gender`,`lastName`,`location`,`song`,`timestamp`,`userAgent`,`userId`,etc can be retrieved from this dataset for further analysis.

## Database Schema

Using the song and logs datasets, we created a star schema optimized for queries on song play analysis. The schema includes the following tables.

### Fact Table

1. __songplays__ 

- This table will contain the records from the log dataset that are associated with song plays. 
- The columns present in this table are `songplay_id`,`start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location` and `user_agent`.

### Dimension Tables

2. __users__

- This table gives information about users in the app. 
- The columns in this table are `user_id`,`first_name`, `last_name`, `gender` and `level`.

3. __songs__

- This table gives information about songs in the music database. 
- The columns in this table are `song_id`, `title`, `artist_id`, `year` and `duration`. 

4. __artists__

- This table gives information about artists in the music database. 
- The columns in this table are `artist_id`, `name`, `location`, `latitude` and `longitude`. 

5. __time__

- This table contains timestamp information from the __songplays__ table broken down into specific units. 
- The columns in this table are `start_time`, `hour`, `day`, `week`, `month`, `year` and `weekday`.

## ETL Pipeline

Based on the database schema described above, the python script `create_table.py` creates the database and all the fact and dimension tables. The `SQL` queries needed to create & drop tables and insert data into the fact and dimension tables are present in the `sql_queries.py` file. The `etl.py` script __extracts__ the required data from the song and logs datasets, __transforms__ the data as per the analytics teams need and as required by the database schema and finally __loads__ the transformed data into the fact and dimension tables. 

## Testing and Validating the ETL Pipeline

Once the `create_tables.py` and `etl.py` scripts run successfully, we can use the `test.ipynb` notebook to verify if the tables have been created correctly and if the data has been inserted correctly. 

Note: The `create_tables.py` script will have to be executed before executing any cell in `test.ipynb` or before executing `etl.py` as the `create_tables.py` script resets the database connection and tables. 



