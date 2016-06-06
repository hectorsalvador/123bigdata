# CMSC 12300 - Computer Science with Applications 3
# Borja Sotomayor, 2013
#

import sys
import random
import numpy
import pickle
from in_manhattan import point_in_Manhattan
from datetime import datetime

from mrjob.job import MRJob

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2
TAXI_PICKUP_TIME = 1
TAXI_DROPOFF_TIME = 2
TAXI_P_LNG = 5
TAXI_P_LAT = 6
TAXI_D_LNG = 9
TAXI_D_LAT = 10


class MRKMeansChooseInitialCentroids(MRJob):

    def __init__(self, args):
        MRJob.__init__(self, args)

    def configure_options(self):
        super(MRKMeansChooseInitialCentroids, self).configure_options()
        self.add_passthrough_option(
            '--k', type='int', help='Number of clusters')
        self.add_passthrough_option(
            '--time', type='str', help='time window to look at')
        self.add_passthrough_option(
            '--triptype', type='str', help='pickup or droppoff ')


    def get_coordinates(self, _, line):
        l = line.split(',')
        if l[0] != 'VendorID' and (l[0] != 'vendor_id') and l[TAXI_P_LAT] != 'pickup_latitude':
            # print l
            if self.options.triptype == "pickup":
                time = l[TAXI_PICKUP_TIME]
                lat1=float(l[TAXI_P_LAT])
                lng1=float(l[TAXI_P_LNG])
                lat2=float(l[TAXI_D_LAT])
                lng2=float(l[TAXI_D_LNG])
            else:
                time = l[TAXI_DROPOFF_TIME]
                lat1=float(l[TAXI_D_LAT])
                lng1=float(l[TAXI_D_LNG])
                lat2=float(l[TAXI_P_LAT])
                lng2=float(l[TAXI_P_LNG])
            date = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
            #check time
            in_time_window = False
            weekday = datetime.weekday(date)
            if self.options.time == "weekday":
                if weekday < 6:
                    if date.hour > 6 and date.hour < 18:
                        in_time_window = True
            if self.options.time == "weeknight":
                if weekday < 6:
                    if date.hour <= 6 or date.hour >= 18:
                        in_time_window = True
            if self.options.time == "weekend":
                if weekday >= 6:
                    in_time_window = True
            # print "Time window: %s" % in_time_window
            if in_time_window and point_in_Manhattan(lat1, lng1) and point_in_Manhattan(lat2, lng2):
                # print "In Time window and manhattan"
                # print "Time %s, Point [%f,%f]" % (time,lat1,lng1)
            # if lat >= 40.477399 and lat <= 40.917577:
            #     if lng >= -74.25909 and lng <=-73.700009:
                yield None, [lat1, lng1]

    def find_ranges(self, _, points):
        minp = maxp = numpy.array(points.next())
        for p in points:
            # print p
            minp = numpy.minimum(minp, p)
            maxp = numpy.maximum(maxp, p)

        yield None, minp.tolist()
        yield None, maxp.tolist()


    def select_centroids(self, _, minmax):
        minp = maxp = numpy.array(minmax.next(), dtype=float)
        for p in minmax:
            minp = numpy.minimum(minp, p)
            maxp = numpy.maximum(maxp, p)

        # Find the K "average" centroids
        k = self.options.k
        step = (maxp-minp) / k

        for i in range(k):
            yield None, (minp + step*i).tolist()

    def steps(self):
        return [self.mr(mapper=self.get_coordinates,
                        combiner=self.find_ranges,
                        reducer=self.select_centroids)]

if __name__ == '__main__':
    MRKMeansChooseInitialCentroids.run()