from mrjob.job import MRJob
from mrjob.step import MRStep
from math import * 
import sys
import pickle
from datetime import datetime, timedelta, time
import csv
from planar import Polygon

NODE_WEEKEND_D = "weekenddropoff.csv"
NODE_WEEKEND_P = "weekendpickup.csv"
NODE_WEEKDAY_D = "weekdaydropoff.csv"
NODE_WEEKDAY_P = "weekdaypickup.csv"
NODE_WEEKNIGHT_D = "weeknightdropoff.csv"
NODE_WEEKNIGHT_P = "weeknightpickup.csv"
 

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2
TAXI_PICKUP_TIME = 1
TAXI_DROPOFF_TIME = 2
TAXI_P_LNG = 5
TAXI_P_LAT = 6
TAXI_D_LNG = 9
TAXI_D_LAT = 10 

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
    '''
    For a given coordinate calculate if it is 
    inside a planar Polygon structure 

    Input:
    x,y : Coordinates for a point (float)
    poly: a polygon structure from Planar library, 
    default is with the coordinates for Manhattan

    Returns 

    Boolean 
    '''
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

def obtain_list(filename):
    '''
    Transform csv file into a list of lists 

    Input: name of csv file, string

    Returns 
    A list of lists with file information
    '''
  list_nodes = []
  with open(filename) as f:
    reader = f.readlines()
    for row in reader:
      row = row.rstrip('\n')
      row = row.split(",")
      one_node = [float(row[0]), float(row[1])] 
      list_nodes.append(one_node)

    f.close()

  return list_nodes

def manhattan_distance(x,y):
  '''
  Calculate a manhattan distance between two coordinate points 

  Input: 
  x: a list with a coordinate point
  y: a list with a coordinate point

  Returns: a float with distance 
  '''
  return sum(abs(a-b) for a,b in zip(x,y))

def transform_time(timeframe):
  '''
  Transforms minutes into hours and minutes
  Example: 154 minutes into 2 hours and 34 minutes

  Input: an integer that states minutes

  Returns: an integer for number of hours and
  for number of minutes 

  '''
  if timeframe >= 60:
    hours = timeframe // 60
    minutes = timeframe % 60 
  else:
    hours = 0
    minutes = timeframe

  return hours, minutes

class MRLabelAddress(MRJob):
  
  def __init__(self, args):
    MRJob.__init__(self, args)

  def configure_options(self):

    super(MRLabelAddress, self).configure_options()
    #Add option to determine the time intervals to get the data (in minutes)
    self.add_passthrough_option(
        '--t', type=int, help='time window to look at in minutes')
  
  def mapper(self, _, line):
    '''
    Maps the data from a Taxi trip file to a
    key determined by coordinates, time of pick-up 
    and bucket of time in a day 
    '''
    str_list = line.split(',')
    try: 
      lat_origin = float(str_list[TAXI_P_LAT])
    except:
      lat_origin = None 
    try:
      lon_origin = float(str_list[TAXI_P_LNG])
    except:
      lon_origin = None
    coord_origin = [lat_origin, lon_origin]
   
    try: 
      time_origin = str_list[TAXI_PICKUP_TIME]
    except:
      time_origin = None  

    try: 
      lat_dest = float(str_list[TAXI_D_LAT])
    except:
      lat_dest = None 
    try:
      lon_dest = float(str_list[TAXI_D_LNG])
    except:
      lon_dest = None

    try: 
      time_destination = str_list[TAXI_DROPOFF_TIME]
    except:
      time_destination = None  
   
    cord_origin = [lat_origin, lon_origin]
    coord_dest = [lat_dest, lon_dest]
    times = [time_origin, time_destination]
    
    timeframe = self.options.t

    if str_list[0] != 'VendorID' and str_list[0] != 'vendor_id' and str_list[TAXI_P_LAT] != 'pickup_latitude':
      date = datetime.strptime(time_origin, '%Y-%m-%d %H:%M:%S')
      
      #Hash time of pickup to a bucket of time according to given
      #timeframe. If pick-up is at 10:47 and you have 30 minutes increments
      #it will hash it to 10:30, if you have 15 minutes increments to 10:45. 

      time_value = ((date.hour * 60) + date.minute ) // timeframe
      hour, minute = transform_time(time_value * timeframe)
      time_i = str(hour) + ":" + str(minute)

      #Determine if trip belongs to weekday, weeknight or weekend
      weekday = datetime.weekday(date)
      if weekday < 6:
        if date.hour > 6 and date.hour < 18:
          period = "Weekday"
        elif date.hour <= 6 or date.hour >= 18:
          period = "Weeknight"
      if weekday >= 6:
        period = "Weekend"
      
      #Check if points are withing Manhattan polygon
      if lat_origin is not None and lon_origin is not None: 
        inside_origin = point_in_Manhattan(lat_origin,lon_origin)
      else:
        inside_origin = False 

      if lat_dest is not None and lon_dest is not None:
        inside_dest = point_in_Manhattan(lat_dest, lon_dest)
      else:
        inside_dest = False

      if inside_origin and inside_dest and \
      time_origin is not None and time_destination is not None:
        trip = [coord_origin, coord_dest, period, time_i]
        yield trip, 1 

  def combiner(self, trip , count):
    '''
    Combine data through adding up number of trips by key
    '''
    times = sum(count)
    yield trip, times


  def reducer(self, trip, times):
    '''
    Reduce data through matching it to the most proximate 
    cluster center and adding frequencies
    '''
    node = None 
    min_distance_origin = float("inf")
    min_distance_dest = float("inf")

    #Obtain cluster centers for origin, destination and type of day.
    if trip[2] == "Weekday":
      centers_origin = NODE_WEEKDAY_P_list
      centers_destination = NODE_WEEKDAY_D_list
    elif trip[2] == "Weekend":
      centers_origin = NODE_WEEKEND_P_list
      centers_destination = NODE_WEEKEND_D_list
    elif trip[2] == "Weeknight":
      centers_origin = NODE_WEEKNIGHT_P_list
      centers_destination = NODE_WEEKNIGHT_D_list

    #Match each trip coordinate to nearest cluster
    for i, center in enumerate(centers_origin):
      distance_origin = manhattan_distance(trip[0], center)
      if distance_origin < min_distance_origin:
        min_distance_origin = distance_origin
        node_origin  = i
    
    for i, center in enumerate(centers_destination):
      distance_dest = manhattan_distance(trip[1], center)
      if distance_dest < min_distance_dest:
        min_distance_dest = distance_dest
        node_destination = i 

    #Create new key with cluster pickup, cluster dropoff, type of day and timeframe
    segment = [node_origin, node_destination,trip[2],trip[3]]
    
    freq = sum(times)
    yield segment, freq


  def combine_node(self, node, frequency):
    '''
    Combine the clusters by adding up frequencies
    '''
    yield node, sum(frequency)

  def reduce_nodes(self, node, frequency):
    '''
    Reduce clusters by adding up frequencies
    '''
    freq = sum(frequency)    
    yield node, freq

  def combine_origin_node(self,node,freq):
    '''
    Save total number of trips by origin cluster
    into a dictionary
    '''
    origin_node = str(node[0])+"-"+str(node[2])+"-"+str(node[3])
    node_sum = node_dict.get(origin_node,0)
    node_sum += int(freq)
    node_dict[origin_node] = node_sum
    
    yield node, freq

  def mapper_final(self,node,freq):
    '''
    Obtain probability of each trip ending up in a 
    destination cluster given total number of trips departing
    from its origin cluster.
    '''
    origin_node = str(node[0])+"-"+str(node[2])+"-"+str(node[3])
    total_sum = node_dict[origin_node]
    percentage = (float(freq) / float(total_sum))*100

    yield node, percentage

  def steps(self):
    return [MRStep(mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
            MRStep(combiner = self.combine_node,
                    reducer = self.reduce_nodes),
            MRStep(mapper = self.combine_origin_node),
            MRStep(mapper = self.mapper_final)]
    
if __name__ == '__main__':
  node_dict = {}
  NODE_WEEKDAY_D_list = obtain_list(NODE_WEEKDAY_D)
  NODE_WEEKDAY_P_list = obtain_list(NODE_WEEKDAY_P)
  NODE_WEEKEND_D_list = obtain_list(NODE_WEEKEND_D)
  NODE_WEEKEND_P_list = obtain_list(NODE_WEEKEND_P)
  NODE_WEEKNIGHT_D_list = obtain_list(NODE_WEEKNIGHT_D)
  NODE_WEEKNIGHT_P_list = obtain_list(NODE_WEEKNIGHT_P)

  MRLabelAddress.run()