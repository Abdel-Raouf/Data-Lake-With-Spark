# Purpose:
putting into practice the following concepts:
- Data modeling (Applying Conceptual Modeling, then Construct Fact and Dimension Tables).
- Database Schema (Apply a specific schema to Fact and Dimension Tables, which suits our Data-Size and Structure => Star-Schema).
- Getting the best of both worlds by using a `Data-Lake`, by applying Dimensional Modelling to Data with high/known value and store low/unknonwn value Data that was previously not available for analytics.
- ETL Pipeline (Construct an ETL Pipeline to Extract Data From Log Files on S3 Bucket(that acts as a Data-Lake), then, apply various transformation needed on the Data using `Apache Spark`, before inserting Data into Fact and Dimensional Tables, then load Data back to `AWS S3` Data-Lake).
- `Apache Spark`
- `Parquet`

# Project Description:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As the data engineer assigned to the project, my role is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Data-Sample:
- **Song Datasets**: all json files can be found under `s3a://udacity-dend/song_data/*/*/*/*.json`. A sample of this files is:
```
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}
```
- **Log Datasets**: all json files can be found under `s3a://udacity-dend/log_data/*/*/*.json`. A sample of this files is:
```
{"artist":null,"auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":0,"lastName":"Summers","length":null,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"GET","page":"Home","registration":1540344794796.0,"sessionId":139,"song":null,"status":200,"ts":1541106106796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
```
# Data Source:
Data is extracted from two types of JSON source files: 
- **songs** data is a subset from the [Million Song Dataset](http://millionsongdataset.com/).
- **users** data generated using [event simulator](https://github.com/Interana/eventsim) Based on the songs dataset. 

# Database Schema:
The schema used for this project is the Star Schema: There is one main fact table containing all the measures associated with each event `songplays`, and 4-dimensional tables `songs`, `artists`, `users` and `time`. Which will contain clean data that is suitable for OLAP(Online Analytical Processing) operations. 

# Architecture Selection:
We used a Data Lake, Due to:
- The wide variety of Data Formats and structures.
- It becomes impossible to conform to a single rigid representation of Data, Due to the emerging of new roles as Data Scientist.
- The `Agile` and `ad-hoc` nature of Data Exploration activities needed by Data Scientists.
- The wide spectrum of Data transformation needed by the Advanced analytics as `Machine Learning`, `Graph Analytics` and `Recommender Systems`.
- Massive parallelism and scalability come out of the box by using `Apache Spark` and `AWS S3` as an Example.
- Using of Columnar Storage as `Parquet`, without expensive `MPP DBs` as `Amazon Redshift`.

# Data Cleaning:
- Filter Log user Data with the `page=NextSong`, to know what song will each user like to listen to next (to help analytics team to continue finding insights about what songs their users love to listen to).
- Converting `Unix time` (Unix epoch format which is the number of seconds since January 1, 1970) to a `Timestamp` using `Spark` DataFrame API.
- Extract hour, day, dayOfMonth, weekOfYear, month, and year From the Timestamp into the `time table`.

# Project Structure:
1. **etl.py** -> this script will load data from `S3 Bucket`, apply transformations on it using `Apache Spark`, then write them back to `S3 Bucket` in a columnar format(Parquet).
2. **dl.cfg** -> Configurations file that Conatins  `AWS Credientials` needed to acess the `S3 Bucket` to load the transformed data on it.
