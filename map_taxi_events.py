from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from haversine import haversine

TAXI_DATE_COL = 1
TAXI_PICKUP_LAT_COL = 6
TAXI_PICKUP_LON_COL = 5
KM_DIST = 0.2
EVENTS = {
    ## other events here
    'sep_1':
        {
        'pickup': (40.7505, -73.9934), #Madison Square G.
        'mindate': datetime(2012,9,1,16,0,0),
        'maxdate': datetime(2012,9,1,16,59,0)
        }
    # other events here
}

def get_trip_info(line):
    try:
        f = line.split(',')
        time = f[TAXI_DATE_COL]
        time = time.strip('"')
        date_obj = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    except:
        return None, None

    lat = float(f[TAXI_PICKUP_LAT_COL])
    lon = float(f[TAXI_PICKUP_LON_COL])
    pickup = (lat, lon)
    
    return date_obj, pickup

def check_event(date, pickup):
    for key, value in EVENTS.items():
        if haversine(value['pickup'], pickup) < KM_DIST:
            if date > value['mindate'] and date < value['maxdate']:
                return key
    return None

class MRGetTripsDuringEvent(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_event_trips,
                    combiner=self.combiner_event_trips,
                    reducer=self.reducer_event_trips)
        ]

    def mapper_get_event_trips(self, _, line):
        '''
        For each line in the spreadsheet, yield a pair with
        visitor_name-status and visitee_name-status
        '''
        date, pickup = get_trip_info(line)
        if date != None:
            event = check_event(date, pickup)
            if event != None:
                yield (event, 1)

    def combiner_event_trips(self, event, counts):
        '''
        Combine to events
        '''
        yield (event, sum(counts))

    def reducer_event_trips(self, event, counts):
        '''
        Reduce to events
        '''
        yield (event, sum(counts))

if __name__ == '__main__':
    MRGetTripsDuringEvent.run()

# 6:00-7:00 provision of EC2 capacity
# 9:00 finished configuring cluster software. Started bootstrap.
# 11:50 finished bootstrapping.
# ~20:00 finished running. Terminating clusters.
# 22:30 print results
# 24:00 Finished. 