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

class MRKMeansUpdateCentroids(MRJob):

    def configure_options(self):
        super(MRKMeansUpdateCentroids, self).configure_options()
        self.add_passthrough_option(
            '--k', type='int', help='Number of clusters')
        self.add_file_option('--centroids')
        self.add_passthrough_option(
            '--time', type='str', help='time window to look at')
        self.add_passthrough_option(
            '--triptype', type='str', help='pickup or droppoff ')

    def get_centroids(self):
        f = open(self.options.centroids)
        centroids = pickle.load(f)
        f.close()
        return centroids

    def assign_cluster(self, _, line):

        l = line.split(',')
        if l[0] != 'VendorID':
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

            if in_time_window and point_in_Manhattan(lat1, lng1) and point_in_Manhattan(lat2, lng2):


                    point = numpy.array([lat1,lng1])
                    centroids = self.get_centroids()
                    
                    distances = [numpy.linalg.norm(point - c) for c in centroids]
                    cluster = numpy.argmin(distances)

                    yield float(cluster), point.tolist()

    def partial_sum(self, cluster, points):
        s = numpy.array(points.next())
        n = 1
        for p in points:
            s += p
            n += 1

        yield cluster, (s.tolist(), n)

    def compute_average(self, cluster, partial_sums):
        SUM, N = partial_sums.next()
        SUM = numpy.array(SUM)
        for ps, n in partial_sums:
            SUM += ps
            N += n

        yield cluster, (SUM / N).tolist()

    def steps(self):
        return [self.mr(mapper=self.assign_cluster,
                        combiner=self.partial_sum,
                        reducer=self.compute_average)]


if __name__ == '__main__':
    MRKMeansUpdateCentroids.run()