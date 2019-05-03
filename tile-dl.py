#!/usr/bin/env python2
# -*- coding: UTF-8 -*-

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

# GLOBALS
mbTiles = object
args = object
earth_circum = 40075.0 # in km

ATTRIBUTION = os.environ.get('METADATA_ATTRIBUTION', '<a href="http://openmaptiles.org/" target="_blank">&copy; OpenMapTiles</a> <a href="http://www.openstreetmap.org/about/" target="_blank">&copy; OpenStreetMap contributors</a>')
VERSION = os.environ.get('METADATA_VERSION', '3.3')

work_dir = '.'

def parse_args():
    parser = argparse.ArgumentParser(description="Download WMTS tiles arount a point.")
    parser.add_argument('-z',"--zoom", help="zoom level. (Default=8)", type=int,default=2)
    parser.add_argument("-m", "--mbtiles", help="mbtiles filename.")
    parser.add_argument("--lat", help="Latitude degrees.",type=float)
    parser.add_argument("--lon", help="Longitude degrees.",type=float)
    parser.add_argument("-r","--radius", help="Download within this radius(km).",type=float)
    parser.add_argument("-d","--dir", help='Output to this directory (use "." for ./output/)')
    parser.add_argument("-g", "--get", help='get WMTS tiles from this URL(Default: Sentinel Cloudless).')
    parser.add_argument("-s", "--summarize", help="Data about each zoom level.",action="store_true")
    return parser.parse_args()

class MBTiles(object):
   def __init__(self, filename):

      self.conn = sqlite3.connect(filename)
      self.conn.row_factory = sqlite3.Row
      self.c = self.conn.cursor()
      self.schemaReady = False

   def __del__(self):
      self.conn.commit()
      self.c.close()
      del self.conn

   def ListTiles(self):
      rows = self.c.execute("SELECT zoom_level, tile_column, tile_row FROM tiles")
      out = []
      for row in rows:
         out.append((row[0], row[1], row[2]))
      return out

   def GetTile(self, zoomLevel, tileColumn, tileRow):
      rows = self.c.execute("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", 
         (zoomLevel, tileColumn, tileRow))
      rows = list(rows)
      if len(rows) == 0:
         raise RuntimeError("Tile not found")
      row = rows[0]
      return row[0]

   def CheckSchema(self):     
      sql = "CREATE TABLE IF NOT EXISTS metadata (name text, value text)"
      self.c.execute(sql)

      sql = "CREATE TABLE IF NOT EXISTS tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob)"
      self.c.execute(sql)

      sql = "CREATE INDEX IF NOT EXISTS tiles_index ON tiles (zoom_level, tile_column, tile_row)"
      self.c.execute(sql)

      self.schemaReady = True

   def GetAllMetaData(self):
      rows = self.c.execute("SELECT name, value FROM metadata")
      out = {}
      for row in rows:
         out[row[0]] = row[1]
      return out

   def SetMetaData(self, name, value):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("UPDATE metadata SET value=? WHERE name=?", (value, name))
      if self.c.rowcount == 0:
         self.c.execute("INSERT INTO metadata (name, value) VALUES (?, ?);", (name, value))

      self.conn.commit()

   def DeleteMetaData(self, name):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("DELETE FROM metadata WHERE name = ?", (name,))
      self.conn.commit()
      if self.c.rowcount == 0:
         raise RuntimeError("Metadata name not found")

   def SetTile(self, zoomLevel, tileColumn, tileRow, data):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("UPDATE tiles SET tile_data=? WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", 
         (sqlite3.Binary(data), zoomLevel, tileColumn, tileRow))
      if self.c.rowcount == 0:
         self.c.execute("INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?);", 
            (zoomLevel, tileColumn, tileRow, sqlite3.Binary(data)))

   def DeleteTile(self, zoomLevel, tileColumn, tileRow):
      if not self.schemaReady:
         self.CheckSchema()

      self.c.execute("DELETE FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", 
         (data, zoomLevel, tileColumn, tileRow))
      self.conn.commit()

      if self.c.rowcount == 0:
         raise RuntimeError("Tile not found")

   def Commit(self):
      self.conn.commit()

   def summarize(self):
     sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;'
     rows = self.c.execute(sql)
     rows = list(rows)
     print('Zoom Levels Found:%s'%len(rows))
     for row in rows:
         print(row)

class WMTS(object):

   def __init__(self, template):
      self.template = template
      self.http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',\
           ca_certs=certifi.where())

   def get(self,z,x,y):
      url = self.template.replace('{z}',str(z))
      url = url.replace('{x}',str(x))
      url = url.replace('{y}',str(y))
      #print(url)
      return(self.http.request("GET",url))
      
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

def get_degree_extent(lat_deg,lon_deg,radius_km,zoom=13):
   (minX,maxX,minY,maxY) = bounds(lat_deg,lon_deg,radius_km,zoom)
   print('minX:%s,maxX:%s,minY:%s,maxY:%s'%(minX,maxX,minY,maxY))
   # following function retures (y,x)
   north_west_point = tools.xy2latlon(minX,minY,zoom)
   south_east_point = tools.xy2latlon(maxX+1,maxY+1,zoom)
   print('north_west:%s south_east:%s'%(north_west_point, south_east_point))
   # returns (west, south, east, north)
   return (north_west_point[1],south_east_point[0],south_east_point[1],north_west_point[0])
  
def to_dir():
   if args.dir != ".":
      prefix = os.path.join(args.dir,'output')
   else:
      prefix = './output'
   for zoom in range(5):
      n = numTiles(zoom)
      for row in range(n):
         for col in range(n):
            this_path = os.path.join(prefix,str(zoom),str(col),str(row)+'.jpeg')
            if not os.path.isdir(os.path.dirname(this_path)):
               os.makedirs(os.path.dirname(this_path))
            raw = get_tile(zoom,col,row)
            with open(this_path,'w') as fp:
               fp.write(raw)


def coordinates2WmtsTilesNumbers(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  return (xtile, ytile)

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

def sat_bbox(lat_deg,lon_deg,zoom,radius):
   magic_number = int(lat_deg * lon_deg * radius)
   bboxes = work_dir + "/bboxes.geojson"
   with open(bboxes,"r") as bounding_geojson:
      data = json.load(bounding_geojson)
      #feature_collection = FeatureCollection(data['features'])
      magic_number_found = False
      for feature in data['features']:
         if feature['properties']['magic_number'] == magic_number:
            magic_number_found = True

   features = [] 
   (west, south, east, north) = get_degree_extent(lat_deg,lon_deg,radius,zoom)
   print('west:%s, south:%s, east:%s, north:%s'%(west, south, east, north))
   west=float(west)
   south=float(south)
   east=float(east)
   north=float(north)
   poly = Polygon([[[west,south],[east,south],[east,north],[west,north],[west,south]]])
   data['features'].append(Feature(geometry=poly,properties={"name":'satellite',\
                           "magic_number":magic_number}))

   collection = FeatureCollection(data['features'])
   bboxes = work_dir + "/bboxes.geojson"
   with open(bboxes,"w") as bounding_geojson:
      outstr = geojson.dumps(collection, indent=2)
      bounding_geojson.write(outstr)

def numTiles(z):
  return(pow(2,z))

def show_metadata():
   metadata = mbTiles.GetAllMetaData()
   for k in metadata:
      print (k, metadata[k])

def get_tile(zoom,tilex,tiley):
   try:
      data = mbTiles.GetTile(zoom, tilex, tiley)
   except RuntimeError as err:
      print (err)
   return(data)

def main():
   global args
   global mbTiles
   args = parse_args()
   if not args.mbtiles and not args.dir:
      print("You must specify Target -- either mbtiles or Directory")
      sys.exit(0)
      sys.exit(0)
   if args.mbtiles and not args.mbtiles.endswith('.mbtiles'):
      args.mbtiles += ".mbtiles"
   if args.mbtiles:
      mbTiles =  MBTiles(args.mbtiles)
   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   if args.dir and args.dir != "":
      if args.dir == ".":
         args.dir = './output'
      if not os.path.isdir(args.dir):
         os.makedirs(args.dir)
   if args.lon == 0.0 and args.lat == 0.0:
      args.lon = -122.14 
      args.lat = 37.46
   if not args.zoom:
      args.zoom = 8
   if not args.radius:
      args.radius = 5
   
      print('inputs to tileXY: lat:%s lon:%s zoom:%s'%(args.lat,args.lon,args.zoom))
      args.x,args.y = tools.tileXY(args.lat,args.lon,args.zoom)
   if  args.get != None:
      print('get specified')
      url = args.get
   else:
      url =  "https://tiles.maps.eox.at/wmts?layer=s2cloudless-2018_3857&style=default&tilematrixset=g&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"
   report(args.lat,args.lon,13,args.radius)
   sat_bbox(args.lat,args.lon,13,args.radius)
   sys.exit(0)
   # Open a WMTS source
   src = WMTS(url)
   for zoom in range(13,1,-1):
      print('zoom level:%s'%zoom)
      download_tiles(src,37.46,-122.14,zoom,5)
      mbTiles.Commit()
   sys.exit(0)


if __name__ == "__main__":
    # Run the main routine
   main()
