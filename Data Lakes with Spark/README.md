## Data Modelling with Data Lake
This Project involves creation of a database for a fictional startup, Sparkify. An AWS EMR cluster is used for this project. 
The data is loaded from AWS S3 into an EMR cluster created on AWS processed to create the tables listed below, once created these are loaded back into S3 as parquet tables. 

These are the following tables created: 
Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, latitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

#### Source Data files 
The files are in json format, these are a subset of the Million Song Dataset. The song folder data contains - Each file contains the song/artists of that song and other artist details. The log folder contains activity logs from the music streaming app for those songs and artists. 

#### How to run this code? 
Make sure you run the files in this order - 
1. etl.py - this helps load the data from the json files from S3 into the respective tables by using pyspark and then write it into S3 as paraquet tables 
2. pyspark_emr_test_tables.ipynb - this pyspark notebook is used to run and test the creation of tables, for a subset of the datatest. Test everything before the script is completed in the etl.py
