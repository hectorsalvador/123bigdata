### Computer Science with Applications III
### Analyzing NYC Taxi Data
### Lauren Dyson, Carlos Grandet, Hector Salvador
### June 2016


import requests
import csv

APIKEY='c2c020da0fa05178acf39b4f9dba3efd'
ARTISTS = ['lady%20gaga', 'drake', 'alicia%20keys', 'lil%20wayne', 'chris%20brown', 'rihanna', 'coldplay', 
	'katy%20perry', 'mariah%20carey', 'justin%20timberlake', 'pink', 'fergie', 'miley%20cyrus', 'snoop%20dogg', 
	'david%20banner', "kanye%20west", "the%20jonas%20brothers", "britney%20spears", 'justin%20bieber', 'one%20direction',
	'eminem', 'pharrell%20williams', 'nicki%20minaj', 'iggy%20azalea', 'adele', 'prince', 'selena%20gomez', 
	'paul%20mccartney', 'avicii', 'pitbull', 'madonna', 'mariah%20carey', 'daft%20punk', 'maroon%205',  'wiz%20khalifa', 
	'jason%20mraz', 'calvin%20harris', 'the%20lumineers', 'linkin%20park', 'macklemore', 'christina%20aguilera',
	'kelly%20clarkson', 'u2', 'radiohead', 'celine%20dion', 'toby%20keith', 'enrique%20iglesias',
	'the%20black%20eyed%20peas','blake%20shelton', 'kenny%20chesney', 'tim%20mcgraw','ariana%20grande']


def go():
	artist_dict = get_ids_lastfm()
	return get_events(artist_dict)

def get_ids_lastfm():
	artist_dict = {}
	for artist in ARTISTS:
		args = {
			"method": "artist.getinfo",
			"artist": artist,
			"api_key": APIKEY,
			"format": 'json'
		}
		url = "http://ws.audioscrobbler.com/2.0/?"
		r = requests.get(url, params=args)
		data = r.json()
		try:
			artist_dict[artist] = data['artist']['mbid']
		except KeyError:
			artist_dict[artist] = None

	return artist_dict


def get_events(artist_dict):
	rv = []
	for artist, mbid in artist_dict.items():
		args = {
			"artist": artist,
			"date": "2009-01-01,2015-07-01",
			"app_id": "uchicago",
			"api_version": "2.0",
		}
		if mbid != None:
			args["mbid"] = "mbid_"+mbid
		url = "http://api.bandsintown.com/artists/"+artist+"/events.json?"

		r = requests.get(
		            url,
		            params = args)
		data = r.json()
		for event in data:
			try:
				city = event["venue"]["city"]
			except TypeError:
				city = None
			try:
				venue = event["venue"]["name"]
			except TypeError:
				venue = None
			if city=="New York" or city=="Manhattan" or venue=="Madison Square Garden"\
			or venue=="Barclays Center" or venue=="Carnegie Hall" or venue=="Terminal 5":
				info = {
					'id': event['id'],
					'datetime': event['datetime'],
					'venue': event['venue']['name'],
					'lat': event['venue']['latitude'],
					'long': event['venue']['longitude'],
					'artist': artist,
					'mbid': mbid
				}
				rv.append(info)

	with open('test.csv', 'w') as f:  # Just use 'w' mode in 3.x
	    w = csv.DictWriter(f, rv[0].keys())
	    w.writeheader()
	    w.writerows(rv)

	return rv
