# DataLake Project
A music streaming startup, Sparkify wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The main task of the project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## How to Run the Project
Intially, fill the dl.cfg file using right aws credentials. 
```
AWS_ACCESS_KEY_ID= YOUR KEY\
AWS_SECRET_ACCESS_KEY=YOUR SECRET KEY 
```

Then create a S3 bucket (sparkify-dend) to upload the output data in parquet format.

Finally, run the following command:

``` 
python etl.py
```

## Structure of the Project 
The following files are found in this project.
- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfgcontains your AWS credentials
- README.md provides discussion on your process and decisions

## Project Datasets
Datasets consists of two sets of data. 
### Song Dataset
Each file is in JSON format and contains metadata about a song and the artist of that song.
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset
The second dataset consists of log files in JSON format generated by an event simulator based on the songs dataset above.

``` {"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}```


## Dataset Schema
Here we create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
#### users - users in the app
user_id, first_name, last_name, gender, level

#### songs - songs in music database
song_id, title, artist_id, year, duration
#### artists - artists in music database
artist_id, name, location, latitude, longitude
#### time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday