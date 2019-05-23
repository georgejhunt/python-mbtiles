#!/usr/bin/env python2
# -*- coding: UTF-8 -*-
# notes to set up this exploration
#  -- the symbolic link satellite.mbtiles is set to source
#  -- output placed in ./work/sat_<name>.mbtiles


# read mbtiles images to viewer
# started from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

import sqlite3
import sys, os
import argparse
import certifi
import urllib3
import tools
import math
from geojson import Feature, Point, FeatureCollection, Polygon
import geojson
from download import MBTiles, WMTS, fetch_quad_for
import shutil
import json


# GLOBALS
mbTiles = object
args = object
earth_circum = 40075.0 # in km
bbox_limits = {} # set by sat_bbox_limits, read by download
src = object
config = {}
config_fn = 'config.json'

ATTRIBUTION = os.environ.get('METADATA_ATTRIBUTION', '<a href="http://openmaptiles.org/" target="_blank">&copy; OpenMapTiles</a> <a href="http://www.openstreetmap.org/about/" target="_blank">&copy; OpenStreetMap contributors</a>')
VERSION = os.environ.get('METADATA_VERSION', '3.3')

work_dir = '/library/www/osm-vector/maplist/assets'

def parse_args():
    parser = argparse.ArgumentParser(description="Download WMTS tiles arount a point.")
    parser.add_argument('-z',"--zoom", help="zoom level". type=int)
    parser.add_argument("-m", "--mbtiles", help="mbtiles filename.")
    parser.add_argument("-n", "--name", help="Output filename.")
    parser.add_argument("--lat", help="Latitude degrees.",type=float)
    parser.add_argument("--lon", help="Longitude degrees.",type=float)
    parser.add_argument("-r","--radius", help="Download within this radius(km).",type=float)
    parser.add_argument("-g", "--get", help='get WMTS tiles from this URL(Default: Sentinel Cloudless).')
    parser.add_argument("-s", "--summarize", help="Data about each zoom level.",action="store_true")
    return parser.parse_args()

class Extract(object):

    def __init__(self, extract, country, city, top, left, bottom, right,
                 min_zoom=0, max_zoom=14, center_zoom=10):
        self.extract = extract
        self.country = country
        self.city = city

        self.min_lon = left
        self.min_lat = bottom
        self.max_lon = right
        self.max_lat = top

        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.center_zoom = center_zoom

    def bounds(self):
        return '{},{},{},{}'.format(self.min_lon, self.min_lat,
                                    self.max_lon, self.max_lat)

    def center(self):
        center_lon = (self.min_lon + self.max_lon) / 2.0
        center_lat = (self.min_lat + self.max_lat) / 2.0
        return '{},{},{}'.format(center_lon, center_lat, self.center_zoom)

    def metadata(self, extract_file):
        return {
            "type": os.environ.get('METADATA_TYPE', 'baselayer'),
            "attribution": ATTRIBUTION,
            "version": VERSION,
            "minzoom": self.min_zoom,
            "maxzoom": self.max_zoom,
            "name": os.environ.get('METADATA_NAME', 'OpenMapTiles'),
            "id": os.environ.get('METADATA_ID', 'openmaptiles'),
            "description": os.environ.get('METADATA_DESC', "Extract from http://openmaptiles.org"),
            "bounds": self.bounds(),
            "center": self.center(),
            "basename": os.path.basename(extract_file),
            "filesize": os.path.getsize(extract_file)
        }

def put_config():
   global config
   with open(config_fn,'w') as cf:
     cf.write(json.dumps(config,indent=2))
 
def get_config():
   global config
   if not os.path.exists(config_fn):
      put_config()

   with open(config_fn,'r') as cf:
     config = json.loads(cf.read())
    
def human_readable(num):
    # return 3 significant digits and unit specifier
    num = float(num)
    units = [ '','K','M','G']
    for i in range(4):
        if num<10.0:
            return "%.2f%s"%(num,units[i])
        if num<100.0:
            return "%.1f%s"%(num,units[i])
        if num < 1000.0:
            return "%.0f%s"%(num,units[i])
        num /= 1000.0

def bounds(lat_deg,lon_deg,radius_km,zoom=13):
   n = 2.0 ** zoom
   tile_kmeters = earth_circum / n
   #print('tile dim(km):%s'%tile_kmeters)
   per_pixel = tile_kmeters / 256 * 1000
   #print('%s meters per pixel'%per_pixel)
   tileX,tileY = coordinates2WmtsTilesNumbers(lat_deg,lon_deg,zoom)
   tile_radius = radius_km / tile_kmeters
   minX = int(tileX - tile_radius) 
   maxX = int(tileX + tile_radius + 1) 
   minY = int(tileY - tile_radius) 
   maxY = int(tileY + tile_radius + 1) 
   return (minX,maxX,minY,maxY)

def record_bbox_debug_info():
   global bbox_limits
   cur_box = regions[region]
   for zoom in range(bbox_zoom_start-1,14):
      xmin,xmax,ymin,ymax = bbox_tile_limits(cur_box['west'],cur_box['south'],\
            cur_box['east'],cur_box['north'],zoom)
      #print(xmin,xmax,ymin,ymax,zoom)
      tot_tiles = mbTiles.CountTiles(zoom)
      bbox_limits[zoom] = { 'minX': xmin,'maxX':xmax,'minY':ymin,'maxY':ymax,                              'count':tot_tiles}
   with open('./work/bbox_limits','w') as fp:
      fp.write(json.dumps(bbox_limits,indent=2))

def get_degree_extent(lat_deg,lon_deg,radius_km,zoom=13):
   (minX,maxX,minY,maxY) = bounds(lat_deg,lon_deg,radius_km,zoom)
   print('minX:%s,maxX:%s,minY:%s,maxY:%s'%(minX,maxX,minY,maxY))
   # following function returns (y,x)
   north_west_point = tools.xy2latlon(minX,minY,zoom)
   south_east_point = tools.xy2latlon(maxX+1,maxY+1,zoom)
   print('north_west:%s south_east:%s'%(north_west_point, south_east_point))
   # returns (west, south, east, north)
   return (north_west_point[1],south_east_point[0],south_east_point[1],north_west_point[0])
  

def coordinates2WmtsTilesNumbers(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  return (xtile, ytile)

def sat_bbox(lat_deg,lon_deg,zoom,radius):
   # Adds a bounding box for the current location, radius
   magic_number = int(lat_deg * lon_deg * radius)
   bboxes = work_dir + "/bboxes.geojson"
   with open(bboxes,"r") as bounding_geojson:
      data = geojson.load(bounding_geojson)
      #feature_collection = FeatureCollection(data['features'])
      magic_number_found = False
      for feature in data['features']:
         if feature['properties'].get('magic_number') == magic_number:
            magic_number_found = True

   features = [] 
   (west, south, east, north) = get_degree_extent(lat_deg,lon_deg,radius,zoom)
   print('west:%s, south:%s, east:%s, north:%s'%(west, south, east, north))
   west=float(west)
   south=float(south)
   east=float(east)
   north=float(north)
   poly = Polygon([[[west,south],[east,south],[east,north],[west,north],[west,south]]])
   if not magic_number_found:
      data['features'].append(Feature(geometry=poly,properties={"name":'satellite',\
                           "magic_number":magic_number}))

   collection = FeatureCollection(data['features'])
   bboxes = work_dir + "/bboxes.geojson"
   with open(bboxes,"w") as bounding_geojson:
      outstr = geojson.dumps(collection, indent=2)
      bounding_geojson.write(outstr)

def download_tiles(src,lat_deg,lon_deg,zoom,radius):
   global mbTiles
   tileX_min,tileX_max,tileY_min,tileY_max = bounds(lat_deg,lon_deg,radius,zoom)
   for tileX in range(tileX_min,tileX_max+1):
      for tileY in range(tileY_min,tileY_max+1):
         print('tileX:%s tileY:%s'%(tileX,tileY))
         try:
            r = src.get(zoom,tileX,tileY)
         except exception as e:
            print(str(e))
            sys.exit(1)
         if r.status == 200:
            mbTiles.SetTile(zoom, tileX, tileY, r.data)
         else:
            print('status returned:%s'%r.status)

def report(lat_deg,lon_deg,zoom,radius):
   tileX_min,tileX_max,tileY_min,tileY_max = bounds(lat_deg,lon_deg,radius,zoom)
   print('minX:%s maxX:%s minY:%s maxY:%s'%bounds(lat_deg,lon_deg,radius,zoom))
   count = ((tileX_max-tileX_min) * (tileY_max-tileY_min))
   print('Tile count:%s Size:%s'%(count,human_readable(count * 5000)))
   print('Time to download: %s minutes'%(count/48))
   #print('or: %s hours'%(count/2880))

def set_up_target_db(name='sentinel'):
   global mbTiles
   mbTiles = None

   # attach to the correct output database
   dbname = 'sat-%s-z0_13.mbtiles'%name
   if not os.path.isdir('./work'):
      os.mkdir('./work')
   dbpath = './work/%s'%dbname
   if not os.path.exists(dbpath):
      shutil.copyfile('./satellite.mbtiles',dbpath) 
   mbTiles = MBTiles(dbpath)
   mbTiles.CheckSchema()
   mbTiles.get_bounds()
   config['last_db'] = dbpath
   put_config()

def do_downloads():
   # Open a WMTS source
   global src # the opened url for satellite images
   try:
      src = WMTS(url)
   except:
      print('failed to open source')
      sys.exit(1)
   set_up_target_db(args.name)
   for zoom in range(args.zoom,14):
      print("new zoom level:%s"%zoom)
      download_tiles(src,args.lat,args.lon,zoom,args.radius)

def main():
   global args
   global mbTiles
   global url
   args = parse_args()
   # Default to standard source
   if not args.mbtiles:
      if config.get('last_db','') != '':
         args.mbtiles = config['last_db']
      else:
         args.mbtiles = './satellite.mbtiles'
   print('mbtiles SOURCE filename:%s'%args.mbtiles)
   if os.path.isfile(args.mbtiles):
      mbTiles  = MBTiles(args.mbtiles)
      mbTiles.get_bounds()
   #if args.mbtiles:
   # mbTiles =  MBTiles(args.mbtiles)
   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   if args.dir and args.dir != "":
      if args.dir == ".":
         args.dir = './output'
      if not os.path.isdir(args.dir):
         os.makedirs(args.dir)
   if not args.lon and not args.lat:
      args.lon = -122.14 
      args.lat = 37.46
   if not args.zoom:
      args.zoom = 10
   if not args.radius:
      args.radius = 15
   if not args.name:
      args.name = 'avni'
   if not args.lon and not args.lat:
      args.lon = -122.14 
      args.lat = 37.46
   print('inputs to tileXY: lat:%s lon:%s zoom:%s'%(args.lat,args.lon,args.zoom))
   args.x,args.y = tools.tileXY(args.lat,args.lon,args.zoom)
   if  args.get != None:
      print('get specified')
      url = args.get
   else:
      url =  "https://tiles.maps.eox.at/wmts?layer=s2cloudless-2018_3857&style=default&tilematrixset=g&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"

   do_downloads() 

if __name__ == "__main__":
    # Run the main routine
   main()
