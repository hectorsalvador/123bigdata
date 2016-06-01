# 123bigdata

## Task A: Analyzing taxi demand in big concerts

### 0. Getting Data 

Concerts
- We obtained a database of 325 concerts in NYC, ranging from 2009 to 2015, using the Bandsintown API: http://www.bandsintown.com/api/overview
- We had to manually verify coordinates of several venues, as they had some defaults that did not match any known venue
- We used “get_bands.py” to get the information in a csv format and turned it into a json file

Taxi rides
- We downloaded two types of data: uber rides and yellow cab rides.
- We took advantage of scripts written by github.com/toddwschneider/nyc-taxi-data to download monthly files of yellow cab rides, from 1/2009 to 12/2015. We also obtained monthly uber rides for the periods of 4/14-9/14 and 1/15 to 6/15.
- Data was uploaded to an S3 bucket: s3://hectorsalvador-spr16-cs123-uchicago-edu/yellow

### 1. Counting taxi rides by event
- We counted how many taxi rides occurred in a three-hour frame since the beginning of each event (as marked by the API). Running one month file with 20 instances on AWS takes about 24 minutes (e.g. using python3 ~/…/map_taxi_events.py -r emr s3://…/yellow_tripdata_2013-03.csv )

### 2. Comparing taxi demand before and after Uber started operations in NYC

## Task B: Destination likelihood

### 0. Getting Data 

Manhattan
- We obtained the coordinate points for the polygon for Manhattan from https://gist.github.com/baygross/5430626

Taxi rides
- We used the same information as the previous task.

### 1. Clustering with K-Means
- For each year (2009-2015), identify a set of cluster centroids (start with K=10) for taxi Pickup and Drop-off locations during three time categories: Weekday daytime, weekday nighttime, and weekends. We only look at trips that start and end within Manhattan. 

### 2. Trip probability
- For each trip starting and ending in Manhattan, determine to which pickup and drop-off cluster does it belong. Reduce on pickup locations and break this down into 30 minute increments. We then calculate the probability (as a relative frequency) of going to any given drop off cluster at that time from that starting region. 
- Look at how the probability of different destinations changes throughout the day from different starting points (e.g. “If I’m in Times Square at midnight, where am I likely to go?” versus “If I’m in Times Square at 7pm…?” How is this different on a weekend versus a weekday?

