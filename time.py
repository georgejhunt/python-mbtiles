#!/usr/bin/env  python
# Calculate the download time for eah region at 10 tiles per second

import os,sys
import json
import shutil
import subprocess
import math

# error out if environment is missing
MR_SSD = os.environ["MR_SSD"]
bbox_limits = {}


REGION_INFO = os.path.join(MR_SSD,'../resources/regions.json')
REGION_LIST = os.environ.get("REGION_LIST")

def coordinates2WmtsTilesNumbers(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(float(lat_deg))
  n = 2.0 ** zoom
  xtile = int((float(lon_deg) + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  ytile = int(n - ytile - 1)
  return (xtile, ytile)

def bbox_tile_limits(west, south, east, north, zoom):
   #print('west:%s south:%s east:%s north:%s zoom:%s'%(west,south,east,north,zoom))
   sw = coordinates2WmtsTilesNumbers(south,west,zoom)
   ne = coordinates2WmtsTilesNumbers(north,east,zoom)
   x_num = ne[0]+1 - sw[0]
   y_num = ne[1]+1 - sw[1]
   #print('ymin:%s ymax:%s'%(sw[1],ne[1]))
   #print('ne_x:%s x_num:%s ne_y:%s y_num:%s'%(ne[0],x_num,ne[1],y_num))
   #print('number of tiles of zoom %s:%s = %d seconds at 8/second'%(zoom,y_num*x_num,y_num*x_num/8))
   #return(xmin,xmax,ymin,ymax)
   return(sw[0],ne[0]+1,sw[1],ne[1]+1)

def record_bbox_debug_info(regions):
   spd = 3600 * 24
   for region in regions.keys():
      cur_box = regions[region]
      tot_tiles = 0
      for zoom in range(1,14):
         xmin,xmax,ymin,ymax = bbox_tile_limits(cur_box['west'],cur_box['south'],\
               cur_box['east'],cur_box['north'],zoom)
         #print(xmin,xmax,ymin,ymax,zoom)
         tiles = (xmax-xmin)*(ymax-ymin)
         bbox_limits[zoom] = { 'minX': xmin,'maxX':xmax,'minY':ymin,'maxY':ymax,                              'count':tot_tiles}
         tot_tiles += tiles
      print region, tot_tiles, tot_tiles/spd, 'days', tot_tiles%spd/3660, 'hrs'
   with open('./work/time_limits','w') as fp:
      fp.write(json.dumps(bbox_limits,indent=2))

rlist = []
outstr = ''
with open(REGION_INFO,'r') as region_fp:
   try:
      data = json.loads(region_fp.read())
   except:
      print("regions.json parse error")
      sys.exit(1)

   record_bbox_debug_info(data['regions'])

