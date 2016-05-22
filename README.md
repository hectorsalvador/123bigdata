# 123bigdata

## Task A: Analyzing taxi demand in big concerts

### 0. Getting Data 

Concerts
- We obtained a database of 325 concerts in NYC, ranging from 2009 to 2015, using the Bandsintown API: http://www.bandsintown.com/api/overview
- We used “get_bands.py” to get the information in a csv format and turned it into a json file for further access from “events.json” 

Taxi rides
- We downloaded two types of data: uber rides and yellow cab rides.
- We took advantage of scripts written by github.com/toddwschneider/nyc-taxi-data to download monthly files of yellow cab rides, from 1/2009 to 12/2015. We also obtained monthly uber rides for the periods of 4/14-9/14 and 1/15 to 6/15.
- Data was uploaded to an S3 bucket: s3://hectorsalvador-spr16-cs123-uchicago-edu/yellow

### 1. Counting taxi rides by event
- We counted how many taxi rides occurred in a three-hour frame since the beginning of each event (as marked by the API)

## Task B: Destination likelihood

### 0. Getting Data 

Manhattan
- We obtained the coordinate points for the polygon for Manhattan from https://gist.github.com/baygross/5430626

Taxi rides
- We used the same information as the previous task.

### 1. Clustering with K-Means

### 2. Trip probability

