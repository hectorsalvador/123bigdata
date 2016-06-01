# CMSC 12300 - Computer Science with Applications 3
# Borja Sotomayor, 2013
#

import sys
import random
import numpy
import pickle
from planar import Polygon
from datetime import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep
#from kmeans_centroid_selector import MRKMeansChooseInitialCentroids
#from kmeans_centroid_updater import MRKMeansUpdateCentroids

### kmeans_centroid_selector

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2
TAXI_PICKUP_TIME = 1
TAXI_DROPOFF_TIME = 2
TAXI_P_LNG = 5
TAXI_P_LAT = 6
TAXI_D_LNG = 9
TAXI_D_LAT = 10
CENTROIDS_FILE="/tmp/emr.kmeans.centroids"


poly_Manhattan = Polygon([
        (40.700292, -74.010773),
        (40.707580, -73.999271),
        (40.710443, -73.978758),
        (40.721762, -73.971977),
        (40.729568, -73.971291),
        (40.733503, -73.973994),
        (40.746834, -73.968072),
        (40.775114, -73.941936),
        (40.778884, -73.942580),
        (40.781906, -73.943589),
        (40.785351, -73.939362),
        (40.789640, -73.936272),
        (40.793149, -73.932238),
        (40.795228, -73.929491),
        (40.801141, -73.928976),
        (40.804877, -73.930907),
        (40.810496, -73.934298),
        (40.834074, -73.934383),
        (40.855371, -73.922281),
        (40.870690, -73.908892),
        (40.878348, -73.928289),
        (40.851151, -73.947258),
        (40.844074, -73.947086),
        (40.828229, -73.955498),
        (40.754019, -74.008713),
        (40.719941, -74.013863),
        (40.718575, -74.013605),
        (40.718802, -74.017038),
        (40.704977, -74.020042),
        (40.700553, -74.016438)
        ])

def point_in_Manhattan(x, y, poly = poly_Manhattan):

    n = len(poly)
    inside = False

    p1x, p1y = poly[0]
    for i in range(n + 1):
        p2x, p2y = poly[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y)*(p2x - p1x)/(p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside

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
        #if (l[0] != 'vendor_id'):
        if (l[0] != 'vendor_id') and (len(l) == 18):
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
        return [MRStep(mapper=self.get_coordinates,
                        combiner=self.find_ranges,
                        reducer=self.select_centroids)]

# if __name__ == '__main__':
#     MRKMeansChooseInitialCentroids.run()

### kmeans_centroid_updater

##########################

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
        if (l[0] != 'vendor_id') and (len(l) == 18):
        #if (l[0] != 'vendor_id'):
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
        return [MRStep(mapper=self.assign_cluster,
                        combiner=self.partial_sum,
                        reducer=self.compute_average)]


# if __name__ == '__main__':
#     MRKMeansUpdateCentroids.run()

###########

def extract_centroids(job, runner):
    c = []
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        print key, value
        c.append(value)
    return c

# If we were running on AWS, we would simply write the centroids
# to an S3 bucket, and then read them from the jobs.
def write_centroids_to_disk(centroids, fname):
    f = open(fname, "w")
    pickle.dump(centroids, f)
    f.close()

def get_biggest_diff(centroids,new_centroids):
    distances = [numpy.linalg.norm(numpy.array(c1) - c2) for c1,c2 in zip(centroids,new_centroids)]
    max_d = max(distances)
    return max_d


def go(args):

    args1 = args
    args1.append("--time")
    args1.append(t)
    args1.append("--triptype")
    args1.append(p)
    print "Starting clusters for %s %s" % (t,p)
    print args1
    choose_centroids_job = MRKMeansChooseInitialCentroids(args=args1)
    print "GOt here"
    with choose_centroids_job.make_runner() as choose_centroids_runner:
        choose_centroids_runner.run()

        centroids = extract_centroids(choose_centroids_job, choose_centroids_runner)
        write_centroids_to_disk(centroids, CENTROIDS_FILE)

        i = 1
        while True:
            print "Iteration #%i" % i
            update_centroids_job = MRKMeansUpdateCentroids(args=args1 + ['--centroids='+CENTROIDS_FILE])
            with update_centroids_job.make_runner() as update_centroids_runner:
                update_centroids_runner.run()

                new_centroids = extract_centroids(update_centroids_job, update_centroids_runner)
                write_centroids_to_disk(new_centroids, CENTROIDS_FILE)

                diff = get_biggest_diff(centroids, new_centroids)

                if diff > 10.0:
                    centroids = new_centroids
                else:
                    output_file = "test_" + str(t) + str(p) + ".txt"
                    print "saving to %s" % output_file

                    write_centroids_to_disk(centroids, output_file)
                    break

                i+=1


if __name__ == '__main__':
    args = sys.argv[1:]
    print args
    # times = ["weekend"]
    # pickup_dropoff = ["pickup"]
    times = ["weekday", "weeknight", "weekend"]
    pickup_dropoff = ["pickup", "dropoff"]
    for t in times:
        for p in pickup_dropoff:
            go(args)
            # args1 = args
            # args1.append("--time")
            # args1.append(t)
            # args.append("--triptype")
            # args.append(p)
            # print "Starting clusters for %s %s" % (t,p)
            # print args
            # choose_centroids_job = MRKMeansChooseInitialCentroids(args=args)
            # print "GOt here"
            # with choose_centroids_job.make_runner() as choose_centroids_runner:
            #     choose_centroids_runner.run()

            #     centroids = extract_centroids(choose_centroids_job, choose_centroids_runner)
            #     write_centroids_to_disk(centroids, CENTROIDS_FILE)

            #     i = 1
            #     while True:
            #         print "Iteration #%i" % i
            #         update_centroids_job = MRKMeansUpdateCentroids(args=args + ['--centroids='+CENTROIDS_FILE])
            #         with update_centroids_job.make_runner() as update_centroids_runner:
            #             update_centroids_runner.run()

            #             new_centroids = extract_centroids(update_centroids_job, update_centroids_runner)
            #             write_centroids_to_disk(new_centroids, CENTROIDS_FILE)

            #             diff = get_biggest_diff(centroids, new_centroids)

            #             if diff > 10.0:
            #                 centroids = new_centroids
            #             else:
            #                 output_file = "test_" + str(t) + str(p) + ".txt"
            #                 print "saving to %s" % output_file

            #                 write_centroids_to_disk(centroids, output_file)
            #                 break

            #             i+=1