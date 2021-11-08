### Data Modelling with Postgres
This Project involves creation of a database for a fictional startup, Sparkify. Postgres database is used for this project. 

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

Source Data files 
The files are in json format, these are a subset of the Million Song Dataset. The song folder data contains - Each file contains the song/artists of that song and other artist details. The log folder contains activity logs from the music streaming app for those songs and artists. 

How to run this code? 
Make sure you run the files in this order - 
1. create_tables.py - this creates all the tables specified above, and on re-run drops them
2. etl.py - this helps load the data from the json files into the respective tables using pandas datagrames
3. test.ipynb - can be used to test the contents of the tables

Other files 
1. sql_queries.py - contains all the SQL queries including create/insert statements. These are then used in the create_tables.py 
2. etl.ipynb - can be used to check the logic, test the ETL process in etl.py with details and a single row of data

Make sure to rerun create_tables.py everytime you change the data 

