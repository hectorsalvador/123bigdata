from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2
EVENTS = {
    'pitbull1':
        {
        'minlat': 40.679665,
        'maxlat': 40.686865,
        'minlng': -73.97979,
        'maxlng': -73.97259,
        'mindate': datetime(2014,9,9,20,0,0),
        'maxdate': datetime(2014,9,10,1,0,0)
        },
    'madonna':
        {
        'minlat': 40.669665,
        'maxlat': 40.676865,
        'minlng': -73.97979,
        'maxlng': -73.97259,
        'mindate': datetime(2014,8,9,12,0,0),
        'maxdate': datetime(2014,8,9,20,0,0)
        },
    'beyonce':
        {
        'minlat': 40.679665,
        'maxlat': 40.686865,
        'minlng': -73.97979,
        'maxlng': -73.97259,
        'mindate': datetime(2014,9,1,8,0,0),
        'maxdate': datetime(2014,9,1,12,0,0)
        }

}

def get_trip_info(line):
    f = line.split(',')
    time = f[UBER_DATE_COL]
    time = time.strip('"')
    if time == 'Date/Time':
        return None, None, None
    lat=float(f[UBER_LAT_COL])
    lng=float(f[UBER_LNG_COL])
    date_obj = datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
    return date_obj, lat, lng

def check_event(date, lat, lng):
    for event, v in EVENTS.items():
        if lat > v['minlat'] and lat < v['maxlat']:
            if lng > v['minlng'] and lng < v['maxlng']:
                if date > v['mindate'] and date < v['maxdate']:
                    return event
    return None

class MRGetTripsDuringEvent(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_event_trips,
                   reducer=self.reducer_event_trips)
        ]

    def mapper_get_event_trips(self, _, line):
        '''
        For each line in the spreadsheet, yield a pair with
        visitor_name-status and visitee_name-status
        '''
        date, lat, lng = get_trip_info(line)
        if date != None:
            event = check_event(date, lat, lng)
            if event != None:
                yield (event, 1)

    def reducer_event_trips(self, event, counts):
        '''
        Reduce to names that have both the status of 'visited'
        and 'visitee'
        '''
        yield(event, sum(counts))

if __name__ == '__main__':

    MRGetTripsDuringEvent.run()
