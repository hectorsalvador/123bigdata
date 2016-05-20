# CMSC 12300 - Computer Science with Applications 3
# Borja Sotomayor, 2013
#

import sys
import random
import numpy
import pickle

from mrjob.job import MRJob

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2

class MRKMeansChooseInitialCentroids(MRJob):

    def __init__(self, args):
        MRJob.__init__(self, args)

    def configure_options(self):
        super(MRKMeansChooseInitialCentroids, self).configure_options()
        self.add_passthrough_option(
            '--k', type='int', help='Number of clusters')

    def get_coordinates(self, _, line):
        l = line.split(',')
        if l[0] != '"Date/Time"':
        #     pass
        # else:
            lat=float(l[UBER_LAT_COL])
            lng=float(l[UBER_LNG_COL])
            if lat >= 40.477399 and lat <= 40.917577:
                if lng >= -74.25909 and lng <=-73.700009:
                    yield None, [lat, lng]

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