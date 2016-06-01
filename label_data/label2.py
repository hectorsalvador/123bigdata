from mrjob.job import MRJob
from mrjob.step import MRStep
from math import * 
import sys
import pickle
from datetime import datetime, timedelta, time


centers_origin  = [[40.691338729, -74.1805413075], [40.738835896, -73.971580809],
                  [40.8033773962, -73.7915374601], [40.7901965392, -73.7433132969]]
centers_destination = [[40.7867994257, -73.9056725958], [40.714259744, -73.8292118349],
                      [40.657326506, -74.2093662651], [40.6880874439, -74.1920473094]]

UBER_DATE_COL = 0
UBER_LAT_COL = 1
UBER_LNG_COL = 2
TAXI_PICKUP_TIME = 1
TAXI_DROPOFF_TIME = 2
TAXI_P_LNG = 5
TAXI_P_LAT = 6
TAXI_D_LNG = 9
TAXI_D_LAT = 10

def manhattan_distance(x,y):
  return sum(abs(a-b) for a,b in zip(x,y))

def transform_time(timeframe):
  if timeframe >= 60:
    hours = timeframe // 60
    minutes = timeframe % 60 
  else:
    hours = 0
    minutes = timeframe

  return hours, minutes
 
def generate_timeframe(timeframe):
  time_list = []

  periods = int((24 * 60) / timeframe)
  for i in range(periods):
    frame = timeframe * i 
    hours, minutes = transform_time(frame)
    current_time = time(int(hours),int(minutes))
    time_list.append(current_time)

  time_list.append(time(0,0))

  return time_list

node_dict = {}


class MRLabelAddress(MRJob):
  
  def __init__(self, args):
    MRJob.__init__(self, args)

  def configure_options(self):
    super(MRLabelAddress, self).configure_options()
    self.add_passthrough_option(
        '--t', type=int, help='time window to look at in minutes')
  
  def mapper(self, _, line):
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
    time_list = generate_timeframe(timeframe)

    if str_list[0] != 'VendorID' and str_list[0] != 'vendor_id' and str_list[TAXI_P_LAT] != 'pickup_latitude':
      date = datetime.strptime(time_origin, '%Y-%m-%d %H:%M:%S')
      time_value = ((date.hour * 60) + date.minute ) // timeframe
      hour, minute = transform_time(time_value * timeframe)
      time_i = str(hour) + ":" + str(minute)

      weekday = datetime.weekday(date)
      if weekday < 6:
        if date.hour > 6 and date.hour < 18:
          period = "Weekday"
        elif date.hour <= 6 or date.hour >= 18:
          period = "Weeknight"
      if weekday >= 6:
        period = "Weekend"
    
      if lat_origin is not None and lon_origin is not None and \
      lat_dest is not None and lon_dest is not None and \
      time_origin is not None and time_destination is not None:
        trip = [coord_origin, coord_dest, period, time_i]
        yield trip, 1 

  def combiner(self, trip , count):
    times = sum(count)
    yield trip, times

  # def reducer_init(filenames):
  #   pass 

  def reducer(self, trip, times):
    node = None 
    min_distance_origin = float("inf")
    min_distance_dest = float("inf")
    self.n_nodes = len(centers_origin)

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

    segment = [node_origin, node_destination,trip[2],trip[3]]
    
    freq = sum(times)
    yield segment, freq


  def combine_node(self, node, frequency):
    yield node, sum(frequency)

  def reduce_nodes(self, node, frequency):
    
    #Create function to store in dictionary
    freq = sum(frequency)    
    yield node, freq

  def combine_origin_node(self,node,freq):
    
    origin_node = str(node[0])+"-"+str(node[2])+"-"+str(node[3])
    node_sum = node_dict.get(origin_node,0)
    node_sum += int(freq)
    node_dict[origin_node] = node_sum
    
    yield node, freq

  # def mapper_final(self,node,freq):
  #   origin_node = str(node[0])+"-"+str(node[2])+"-"+str(node[3])
  #   total_sum = node_dict[origin_node]
  #   percentage = (float(freq) / float(total_sum))*100

  #   yield node, percentage

  def steps(self):
    return [MRStep(mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
            MRStep(combiner = self.combine_node,
                    reducer = self.reduce_nodes),
            MRStep(mapper = self.combine_origin_node)]
            # MRStep(mapper = self.mapper_final)]
    
if __name__ == '__main__':
    MRLabelAddress.run()