{\rtf1\ansi\ansicpg1252\cocoartf1404\cocoasubrtf460
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 1) Run get_bands.py to obtain .csv with events.\
2) Manually resolve coordinates of cvs files, sorting them to make task easier. Results are on sorted_bands.csv\
3) Turn .csv to .json with csv_to_json.py for easy access from map_taxi_events.py\
4) Run python3 map_taxi_events.py <path/to/file.csv>  or python3 map_taxi_events.py -r emr <name/of/s3bucket/folder> when using AWS}