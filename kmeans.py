# CMSC 12300 - Computer Science with Applications 3
# Borja Sotomayor, 2013
#

import sys
import random
import numpy
import pickle
import csv

from mrjob.job import MRJob
from kmeans_centroid_selector import MRKMeansChooseInitialCentroids
from kmeans_centroid_updater import MRKMeansUpdateCentroids
from kmeans_filter_points import MRInManhattan


CENTROIDS_FILE="/tmp/emr.kmeans.centroids"
POINTS_FILE = "/tmp/emr.kmeans.points.csv"
DIR = "output/"


def write_output(job, runner, fname):
    '''
    Write output of final clusters to csv
    '''
    with open(fname, "w") as f:
        writer = csv.writer(f)
        for line in runner.stream_output():
            key, value = job.parse_output_line(line)
            print key,value
            # r = str(key)+","+str(value)
            writer.writerow(value)
    print "wrote out output"

def extract_points(job, runner, fname):
    # print "writing points"

    with open(fname, "w") as f:
        writer = csv.writer(f)
        for line in runner.stream_output():
            key, value = job.parse_output_line(line)
            r = str(key)+","+str(value)
            writer.writerow(value)


def extract_centroids(job, runner):
    c = []
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        c.append(value)
    return c

def write_centroids_to_disk(centroids, fname):
    f = open(fname, "w")
    pickle.dump(centroids, f)
    f.close()

def get_biggest_diff(centroids,new_centroids):
    distances = [numpy.linalg.norm(numpy.array(c1) - c2) for c1,c2 in zip(centroids,new_centroids)]
    max_d = max(distances)
    return max_d

if __name__ == '__main__':
    times = ["weekday", "weeknight", "weekend"]
    pickup_dropoff = ["pickup","dropoff"]

    for t in times:
        for p in pickup_dropoff:
            args = sys.argv[1:-1]
            init_file = sys.argv[-1]
            print init_file
            args.append("--time")
            args.append(t)
            args.append("--triptype")
            args.append(p)
            print "Starting clusters for %s %s" % (t,p)

            print args+[init_file]
            filter_points_job = MRInManhattan(args=args+[init_file])

            with filter_points_job.make_runner() as filter_points_runner:
                filter_points_runner.run()

                extract_points(filter_points_job,filter_points_runner,POINTS_FILE)
            print "points filtered"
            choose_centroids_job = MRKMeansChooseInitialCentroids(args=args+[POINTS_FILE])
            with choose_centroids_job.make_runner() as choose_centroids_runner:
                choose_centroids_runner.run()

                centroids = extract_centroids(choose_centroids_job, choose_centroids_runner)
                write_centroids_to_disk(centroids, CENTROIDS_FILE)

                i = 1
                while True:
                    print "Iteration #%i" % i
                    update_centroids_job = MRKMeansUpdateCentroids(args=args + [POINTS_FILE]+['--centroids='+CENTROIDS_FILE])
                    with update_centroids_job.make_runner() as update_centroids_runner:
                        update_centroids_runner.run()

                        new_centroids = extract_centroids(update_centroids_job, update_centroids_runner)
                        write_centroids_to_disk(new_centroids, CENTROIDS_FILE)

                        diff = get_biggest_diff(centroids, new_centroids)
                        if i < 10:
                            centroids = new_centroids
                        else:
                            output_file = DIR + str(t) + str(p) + "_taxi.csv"
                            print "saving to %s" % output_file
                            write_output(update_centroids_job,update_centroids_runner,output_file)
                            break

                        i+=1