import csv
import json
from datetime import datetime

def go():

	f = open('sorted_bands.csv', 'r')
	reader = csv.DictReader(f)
	d = {}

	for row in reader:
		name = "{} @{}, {}".format(row['artist'], row['venue'], row['datetime'])
		d[name] = {}
		d[name]['pickup'] = (eval(row['lat']), eval(row['lon']))
		d[name]['date'] = row['datetime']
		year = datetime.strptime(row['datetime'], '%Y-%m-%dT%H:%M:%S').year

	with open('sorted_bands.json'.format(year), 'w') as fp:
		json.dump(d, fp)


if __name__ == '__main__':
    go()

