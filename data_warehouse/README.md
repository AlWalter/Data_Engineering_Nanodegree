<h3>Data Warehouse ETL using AWS RedShift</h3>

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

<h3>Project Description</h3>
This project buils an ETL pipeline for a database hosted on AWS Redshift. It loads data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

<h3>Project Datasets</h3>

The data is queried from s3 buckets hosten at AWS
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data


<h3>Song Dataset</h3>
The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

And below is an example of what the data in a log file
![](./img/log-data.png)

<h3>Files</h3>

<ul>
<li>create_tables.py -  python script to create fact and dimension tables for the star schema in Redshift.</li>
<li>sql_queries.py - python script which load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.</li>
<li>etl.py - contains SQL statements, which will be imported into the two other files above.</li>
<li>dwh.cfg - configuration file contais variables for the RedShift cluster and S3 access.</li>
</ul>

<h3>Database Schema</h3>

The schema design represents a star schema, which consists of one fact table:
<ul>
<li>songplays</li>
</ul>
and dimension tables:
<ul>
<li>users</li>
<li>time</li>
<li>songs</li>
<li>artist</li>
</ul>


![](./img/star_schema.png)


<h3>Instructions on running the application<h3>

<ul>
<li>You must have an access to an AWS RedShift cluster</li>
<li>Setup the dwh.cfg file</li>
<li>run the create_tables.py to create the tables</li>
<li>run etl.py to process the data</li>
</ul>
