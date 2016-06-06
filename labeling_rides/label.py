from mrjob.job import MRJob
from math import *

centers = [[40.691338729, -74.1805413075], [40.738835896, -73.971580809]]

def manhattan_distance(x,y):
  return sum(abs(a-b) for a,b in zip(x,y))

class MRLabelAddress(MRJob):
  
  def mapper(self, _, line):
    str_list = line.split(',')
    try: 
      lat = float(str_list[1])
    except:
      lat = None 
    try:
      lon = float(str_list[2])
    except:
      lon = None
    coord = [[lat, lon],1]
    if lat is not None and lon is not None:
        yield coord, 0

  def combiner(self, coord , counts):
    yield coord, sum(counts)

  def reducer(self, coord, counts):
    node = None 
    min_distance = float("inf")
    for i, center in enumerate(centers):
      distance = manhattan_distance(coord[0], center)
      if distance < min_distance:
        min_distance = distance
        node  = i 

    yield coord, node


if __name__ == '__main__':
    MRLabelAddress.run()



class MRClearDoubts(MRJob):

  def get_coordinates(self, _, line):
    l = line.split(",")

    if len(l) == 1:
        return
    
    yield None, [float(x) for x in l]

  def find_ranges(self, _, points):
    minp = maxp = numpy.array(points.next())
    print(minp, maxp)
    for p in points:
        print("p", p)
        minp = numpy.minimum(minp, p)
        maxp = numpy.maximum(maxp, p)

    print("R", minp, maxp)
    yield None, minp.tolist()
    yield None, maxp.tolist()


  def select_centroids(self, _, minmax):
    minp = maxp = numpy.array(minmax.next(), dtype=float)
    print("reducer")
    print(minp, maxp)
    for p in minmax:
        print("p", p)
        minp = numpy.minimum(minp, p)
        maxp = numpy.maximum(maxp, p)

    # Find the K "average" centroids
    k = 2
    step = (maxp-minp) / k
    print("step", step)

    for i in range(k):
        yield None, (minp + step*i).tolist()

  def steps(self):
    return [MRStep(mapper=self.get_coordinates,
                    combiner=self.find_ranges,
                    reducer=self.select_centroids)]