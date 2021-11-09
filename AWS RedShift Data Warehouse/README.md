## Data Modelling with AWS
This Project involves creation of a database for a fictional startup, Sparkify. AWS redshift database is used for this project. 

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
1. create_tables.py - this creates all the tables specified above, and on re-run drops them
2. etl.py - this helps load the data from the json files into the respective tables using pandas datagrames
3. t_check_the_flow.ipynb - can be used to test the contents of the tables

#### Other files 
1. z_create_the_cluster.ipynb - Used to create the cluster, and delete it after use 
2. z_test_out_the_table_Creation.ipynb - can be used to dummy test the logic and create the tables as a test. These are then copied to the etl.py after extensive iteration and testing 

Make sure to rerun create_tables.py everytime you change the data 
