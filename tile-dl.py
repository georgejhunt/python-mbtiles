#!/usr/bin/env python2
# read mbtiles images to viewer
# started from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

import sqlite3
import sys, os
import argparse
import StringIO
import certifi
import urllib3
import tools
import subprocess
import math

# GLOBALS
mbTiles = object
args = object

def parse_args():
    parser = argparse.ArgumentParser(description="Download WMTS tiles arount a point.")
    parser.add_argument('-z',"--zoom", help="zoom level. (Default=8)", type=int,default=2)
    parser.add_argument("-m", "--mbtiles", help="mbtiles filename.")
    parser.add_argument("--lat", help="Latitude degrees.",type=float)
    parser.add_argument("--lon", help="Longitude degrees.",type=float)
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
         (data, zoomLevel, tileColumn, tileRow))
      if self.c.rowcount == 0:
         self.c.execute("INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?);", 
            (zoomLevel, tileColumn, tileRow, data))

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
   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   if args.mbtiles and not args.mbtiles.endswith('.mbtiles'):
         args.mbtiles += ".mbtiles"
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
   
      print('inputs to tileXY: lat:%s lon:%s zoom:%s'%(args.lat,args.lon,args.zoom))
      args.x,args.y = tools.tileXY(args.lat,args.lon,args.zoom)
   if  args.get != None:
      print('get specified')
      url = args.get
   else:
      url =  "https://tiles.maps.eox.at/wmts?layer=s2cloudless-2018_3857&style=default&tilematrixset=g&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"
   # Open a WMTS source
   src = WMTS(url)
   try:
      r = src.get(4,2,3)
   except exception as e:
      print(str(e))
      sys.exit(1)
   print(r.status)


if __name__ == "__main__":
    # Run the main routine
   main()
