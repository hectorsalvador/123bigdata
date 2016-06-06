
1) Run get_bands.py to obtain .csv with events.

2) Manually resolve coordinates of cvs files, sorting them to make task easier. Results are on sorted_bands.csv

3) Turn .csv to .json with csv_to_json.py for easy access from map_taxi_events.py

4) Run python3 map_taxi_events.py <path/to/file.csv>  or python3 map_taxi_events.py -r emr <name/of/s3bucket/folder> when using AWS