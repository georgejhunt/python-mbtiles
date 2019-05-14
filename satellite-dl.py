#!/usr/bin/env python2
# Download satellite imagess from Sentinel Cloudless

# help from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

import sqlite3
import sys, os
import argparse
from PIL import Image
import StringIO
import curses
import certifi
import urllib3
import tools
import subprocess
import json
import math
import uuid
import shutil

# Download source of satellite imagry
url =  "https://tiles.maps.eox.at/wmts?layer=s2cloudless-2018_3857&style=default&tilematrixset=g&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={z}&TileCol={x}&TileRow={y}"
src = object # the open url source
# tiles smaller than this are probably ocean
threshold = 2000

# GLOBALS
mbTiles = object
args = object
bounds = {}
regions = {}
bbox_zoom_start = 8
bbox_limits = {}
stdscr = object # cursors object for progress feedback

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
      sql = 'CREATE TABLE IF NOT EXISTS map (zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_id TEXT,grid_id TEXT)'
      self.c.execute(sql)

      sql = 'CREATE TABLE IF NOT EXISTS images (tile_data blob,tile_id text)'
      self.c.execute(sql)

      sql = 'CREATE VIEW tiles AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, images.tile_data AS tile_data FROM map JOIN images ON images.tile_id = map.tile_id'
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

      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if tile_id: 
         self.c.execute("UPDATE images SET tile_data=? WHERE tile_id = ?;", (data, tilee_id))
      else: # this is not an update
         tile_id = uuid.uuid().hex
         self.c.execute("INSERT INTO images ( tile_data,tile_id) VALUES ( ?, ?);", (data,tile_id))
         self.c.execute("INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?);", 
            (zoomLevel, tileColumn, tileRow, tile_id))

   def DeleteTile(self, zoomLevel, tileColumn, tileRow):
      if not self.schemaReady:
         self.CheckSchema()

      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if not tile_id:
         raise RuntimeError("Tile not found")

      self.c.execute("DELETE FROM images WHERE tile_id = ?;",tile_id) 
      self.c.execute("DELETE FROM map WHERE tile_id = ?;",tile_id) 
      self.conn.commit()

   def TileExists(self, zoomLevel, tileColumn, tileRow):
      if not self.schemaReady:
         self.CheckSchema()

      sql = 'select tile_id from map where zoom_level = ? and tile_column = ? and tile_row = ?'
      row = self.c.execute(sql,(zoomLevel, tileColumn, tileRow)
      if self.c.rowcount == 0:
         return None
      return row[0][0]

   def DownloadTile(self, zoomLevel, tileColumn, tileRow):
      # if the tile already exists, do nothing
      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if tile_id:
         return 
      try:
         r = src.get(zoomLevel,tileColumn,tileRow)
      except Exception as e:
         raise RuntimeError("Source data failure;%s"%e)
         
      if r.status == 200:
         self.SetTile(zoomLevel, tileColumn, tileRow, r.data)
      else:
         print('status returned:%s'%r.status)

   def Commit(self):
      self.conn.commit()

   def get_bounds(self):
     global bounds
     sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;'
     resp = self.c.execute(sql)
     rows = resp.fetchall()
     for row in rows:
         bounds[row['zoom_level']] = { 'minX': row['min(tile_column)'],\
                                  'maxX': row['max(tile_column)'],\
                                  'minY': row['min(tile_row)'],\
                                  'maxY': row['max(tile_row)'],\
                                 }
     outstr = json.dumps(bounds,indent=2)
     #print('bounds:%s'%outstr)

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

def parse_args():
    parser = argparse.ArgumentParser(description="Display mbtile image.")
    parser.add_argument('-z',"--zoom", help="zoom level. (Default=2)", type=int,default=2)
    parser.add_argument("-x",  help="tileX", type=int)
    parser.add_argument("-y",  help="tileY", type=int)
    parser.add_argument("-m", "--mbtiles", help="mbtiles filename.")
    parser.add_argument("-s", "--summarize", help="Data about each zoom level.",action="store_true")
    parser.add_argument("--lat", help="Latitude degrees.",type=float)
    parser.add_argument("--lon", help="Longitude degrees.",type=float)
    parser.add_argument("-d","--dir", help='Output to this directory (use "." for ./output/)')
    parser.add_argument("-g", "--get", help='get WMTS tiles from this URL(of "." for Sentinel Cloudless).')
    return parser.parse_args()

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

def get_regions():
   global regions
   # error out if environment is missing
   MR_SSD = os.environ["MR_SSD"]

   REGION_INFO = os.path.join(MR_SSD,'../resources/regions.json')
   with open(REGION_INFO,'r') as region_fp:
      try:
         data = json.loads(region_fp.read())
         regions = data['regions']
      except:
         print("regions.json parse error")
         sys.exit(1)
   
def key_parse(stdscr):
   # permits viewing of individual image tiles (-x,-y,-y parameters)
   global args
   global mbTiles
   zoom =2
   if args.zoom:
      zoom = args.zoom      
   state = { 'zoom': zoom}
   if args.x:
      state['tileX'] = args.x
   else:
      state['tileX'] = bounds[state['zoom']]['minX']
   if args.y:
      state['tileY'] = 2 ** zoom  - args.y - 1
   else:
      state['tileY'] = bounds[state['zoom']]['minY']
   while 1:
      try:
         raw = mbTiles.GetTile(state['zoom'],state['tileX'],state['tileY'])
         stdscr.clear()
         stdscr.addstr(0,0,'zoom:%s lon:%s lat:%s'%(state['zoom'],state['tileX'],state['tileY']))
         stdscr.addstr(0,40,'Size of tile:%s'%len(raw))
         stdscr.refresh() 
         proc = subprocess.Popen(['killall','display'])
         proc.communicate()
         image = Image.open(StringIO.StringIO(raw))
         image.show()
      except:  
         print('Tile not found. x:%s y:%s'%(state['tileX'],state['tileY']))

      n = numTiles(state['zoom'])
      ch = stdscr.getch()
      if ch == ord('q'):
         proc = subprocess.Popen(['killall','display'])
         proc.communicate()
         break  # Exit the while()
      if ch == curses.KEY_UP:
         if not state['tileY'] == bounds[state['zoom']]['minY']:
            state['tileY'] -= 1
      elif ch == curses.KEY_RIGHT:
         if not state['tileX'] == bounds[state['zoom']]['maxX']-1:
            state['tileX'] += 1
      elif ch == curses.KEY_LEFT:
         if not state['tileX'] == bounds[state['zoom']]['minX']:
            state['tileX'] -= 1
      elif ch == curses.KEY_DOWN:
         if not state['tileY'] == bounds[state['zoom']]['minY']-1:
            state['tileY'] += 1
      elif ch == ord('='):
         if not state['zoom'] == 13:
            state['tileX'] *= 2
            state['tileY'] *= 2
            state['zoom'] += 1
      elif ch == ord('-'):
         if not state['zoom'] == 1:
            state['tileX'] /= 2
            state['tileY'] /= 2
            state['zoom'] -= 1

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

def fetch_quad_for(tileX, tileY, zoom):
   # get 4 tiles for zoom+1
   mbTiles.DownloadTile(zoom+1,tileX*2,tileY*2)
   mbTiles.DownloadTile(zoom+1,tileX*2+1,tileY*2)
   mbTiles.DownloadTile(zoom+1,tileX*2,tileY*2+1)
   mbTiles.DownloadTile(zoom+1,tileX*2+1,tileY*2+1)
   mbTiles.Commit()
  
def download_region(region):
   global src # the opened url for satellite images
 
   # print some summary info for this region
   stdscr.addstr(1,0,"ZOOM")
   stdscr.addstr(0,15,region)
   stdscr.addstr(1,10,'PRESENT')
   stdscr.addstr(1,20,'NEEDED')
   stdscr.addstr(1,30,'PERCENT')
   stdscr.addstr(1,50,"DAYS")

   # 
   cur_box = regions[region]
   for zoom in range(14):
      stdscr.addstr(zoom+2,0,str(zoom))
      xmin,xmax,ymin,ymax = bbox_tile_limits(cur_box['west'],cur_box['south'],\
            cur_box['east'],cur_box['north'],zoom)
      #print(xmin,xmax,ymin,ymax,zoom)
      bbox_limits[zoom] = { 'minX': xmin,'maxX':xmax,'minY':ymin,'maxY':ymax}

      if bounds.get(zoom,-1) == -1: continue
      tiles = (bounds[zoom]['maxX']-bounds[zoom]['minX'])*\
              (bounds[zoom]['maxY']-bounds[zoom]['minY'])
      stdscr.addstr(zoom+2,10,str(tiles))
      
   for zoom in range(bbox_zoom_start,13):
      stdscr.addstr(zoom+2,0,str(zoom))
      tiles = (xmax-xmin)*(ymax-ymin)
      stdscr.addstr(zoom+2,20,str(tiles))
      hours = tiles/3600/24.0
      stdscr.addstr(zoom+2,50,'%0.2f'%hours)

   # Open a WMTS source
   src = WMTS(url)
   # Look at current tiles, if larges than threshold, get the 4 tiles a zoom+1
   for zoom in range(bbox_zoom_start-1,14):
      processed = 0
      for ytile in range(bbox_limits[zoom]['minY'],bbox_limits[zoom]['maxY']):
         for xtile in range(bbox_limits[zoom]['minX'],bbox_limits[zoom]['maxX']):
            processed += 4
            raw = mbTiles.GetTile(zoom, xtile, ytile)
            if len(raw) > threshold:
               fetch_quad_for(xtile, ytile, zoom)
            if processed % 10 == 0:
               stdscr.addstr(zoom+2,60,"%d"%processed)


         break         

def download(scr):
    global stdscr
    stdscr = scr
    k=0
    # Clear and refresh the screen for a blank canvas
    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)

    # Loop where k is the last character pressed
    while (k != ord('q')):
         #for region in regions.keys():

         download_region('central_america')
         # Refresh the screen
         stdscr.refresh()

         # Wait for next input
    k = stdscr.getch()


def main():
   global args
   global mbTiles
   args = parse_args()
   get_regions()
   if not args.mbtiles:
      args.mbtiles = 'satellite.mbtiles'
   mbTiles  = MBTiles(args.mbtiles)
   mbTiles.get_bounds()

   print('mbtiles filename:%s'%args.mbtiles)
   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   if args.x and args.y:
      curses.wrapper(key_parse)
      sys.exit(0)
   if args.lon and args.lat:
      if not args.zoom:
         args.zoom = 2
      print('inputs to tileXY: lat:%s lon:%s zoom:%s'%(args.lat,args.lon,args.zoom))
      args.x,args.y = tools.tileXY(args.lat,args.lon,args.zoom)
   if  args.get != None:
      print('get specified')
      set_url()
   if args.dir != None:
      to_dir()
      sys.exit(0)

   curses.wrapper(download) 
   

if __name__ == "__main__":
    # Run the main routine
   main()