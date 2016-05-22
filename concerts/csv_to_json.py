import csv
import json
from datetime import datetime

def go():

	f = open('test.csv', 'r')
	reader = csv.DictReader(f)
	d = {}

	for row in reader:
		name = "{}@{}, {}".format(row['artist'], row['venue'], row['datetime'])
		d[name] = {}
		d[name]['pickup'] = (eval(row['lat']), eval(row['lon']))
		d[name]['date'] = row['datetime']

	with open('events.json', 'w') as fp:
		json.dump(d, fp)


if __name__ == '__main__':
    go()

