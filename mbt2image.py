#!/usr/bin/env python2
# read mbtiles images to viewer -- SUPPLANTED BY download.py
# started from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

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

# GLOBALS
mbTiles = object
args = object
bounds = {}

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
     print('bounds:%s'%outstr)

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
    parser.add_argument("-x",  help="tileX", type=int, default=1)
    parser.add_argument("-y",  help="tileY", type=int, default=2)
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

def key_parse(stdscr):
   global args
   global mbTiles
   state = { 'zoom':2}
   state['tileX'] = bounds[state['zoom']]['minX']
   state['tileY'] = bounds[state['zoom']]['minY']
   while 1:
      stdscr.clear()
      stdscr.addstr(0,0,'zoom:%s lon:%s lat:%s'%(state['zoom'],state['tileX'],state['tileY']))
      stdscr.refresh() 
      try:
         raw = mbTiles.GetTile(state['zoom'],state['tileX'],state['tileY'])
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
         if not state['tileX'] == bounds[state['zoom']]['maxX']:
            state['tileX'] += 1
      elif ch == curses.KEY_LEFT:
         if not state['tileX'] == bounds[state['zoom']]['minX']:
            state['tileX'] -= 1
      elif ch == curses.KEY_DOWN:
         if not state['tileY'] == bounds[state['zoom']]['maxY']:
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


def wrapper(func, *args, **kwds):
    """Wrapper function that initializes curses and calls another function,
    restoring normal keyboard/screen behavior on error.
    The callable object 'func' is then passed the main window 'stdscr'
    as its first argument, followed by any other arguments passed to
    wrapper().
    """

    try:
        # Initialize curses
        stdscr = curses.initscr()

        # Turn off echoing of keys, and enter cbreak mode,
        # where no buffering is performed on keyboard input
        curses.noecho()
        curses.cbreak()

        # In keypad mode, escape sequences for special keys
        # (like the cursor keys) will be interpreted and
        # a special value like curses.KEY_LEFT will be returned
        stdscr.keypad(1)

        # Start color, too.  Harmless if the terminal doesn't have
        # color; user can test with has_color() later on.  The try/catch
        # works around a minor bit of over-conscientiousness in the curses
        # module -- the error return from C start_color() is ignorable.
        try:
            curses.start_color()
        except:
            pass

        return func(stdscr, *args, **kwds)
    finally:
        # Set everything back to normal
        if 'stdscr' in locals():
            stdscr.keypad(0)
            curses.echo()
            curses.nocbreak()
            curses.endwin() 

def main():
   global args
   global mbTiles
   args = parse_args()
   if not args.mbtiles:
      args.mbtiles = 'satellite.mbtiles'
   mbTiles  = MBTiles(args.mbtiles)
   if args.summarize:
      mbTiles.summarize()
      sys.exit(0)
   print('mbtiles filename:%s'%args.mbtiles)
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
      
   mbTiles.get_bounds()
   wrapper(key_parse) 
   

if __name__ == "__main__":
    # Run the main routine
   main()
