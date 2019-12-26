#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Assemble an output mbtiles database from multiple input sources (perhaps from openmaptiles.org)

# help from https://github.com/TimSC/pyMbTiles/blob/master/MBTiles.py

import sqlite3
import sys, os
import argparse
import curses
import urllib3
#import tools
import subprocess
import json
import math
import uuid
import shutil
from multiprocessing import Process, Lock
import time

class MBTiles():
   def __init__(self, filename):
      self.conn = sqlite3.connect(filename)
      self.conn.row_factory = sqlite3.Row
      self.conn.text_factory = str
      self.c = self.conn.cursor()
      self.schemaReady = False

   def __del__(self):
      self.conn.commit()
      self.c.close()
      del self.conn

   def GetTile(self, zoomLevel, tileColumn, tileRow):
      rows = self.c.execute("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", 
         (zoomLevel, tileColumn, tileRow))
      rows = list(rows)
      if len(rows) == 0:
         raise RuntimeError("Tile not found")
      row = rows[0]
      return row[0]

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
         tile_id = uuid.uuid4().hex
         operation = 'update images'
         self.c.execute("DELETE FROM images  WHERE tile_id = ?;", ([tile_id]))
         self.c.execute("INSERT INTO images (tile_data,tile_id) VALUES ( ?, ?);", (sqlite3.Binary(data),tile_id))
         if self.c.rowcount != 1:
            raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
         self.c.execute("""UPDATE map SET tile_id=? where zoom_level = ? AND 
               tile_column = ? AND tile_row = ?;""", 
            (tile_id, zoomLevel, tileColumn, tileRow))
         if self.c.rowcount != 1:
            raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
         self.conn.commit()
         return
      else: # this is not an update
         tile_id = uuid.uuid4().hex
         self.c.execute("INSERT INTO images ( tile_data,tile_id) VALUES ( ?, ?);", (sqlite3.Binary(data),tile_id))
         if self.c.rowcount != 1:
            raise RuntimeError("Insert image failure")
         operation = 'insert into map'
         self.c.execute("INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?);", 
            (zoomLevel, tileColumn, tileRow, tile_idi))
      if self.c.rowcount != 1:
         raise RuntimeError("Failure %s RowCount:%s"%(operation,self.c.rowcount))
      self.conn.commit()
   

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
      self.c.execute(sql,(zoomLevel, tileColumn, tileRow))
      row = self.c.fetchall()
      if len(row) == 0:
         return None
      return str(row[0][0])

   def DownloadTile(self, zoomLevel, tileColumn, tileRow, lock):
      # if the tile already exists, do nothing
      tile_id = self.TileExists(zoomLevel, tileColumn, tileRow)
      if tile_id:
         print('tile already exists -- skipping')
         return 
      try:
         #wmts_row = int(2 ** zoomLevel - tileRow - 1)
         r = src.get(zoomLevel,tileColumn,tileRow)
      except Exception as e:
         raise RuntimeError("Source data failure;%s"%e)
         
      if r.status == 200:
         lock.acquire()
         self.SetTile(zoomLevel, tileColumn, tileRow, r.data)
         self.conn.commit()
         lock.release()
      else:
         print('Sat data error, returned:%s'%r.status)

   def Commit(self):
      self.conn.commit()

def main():
   global mbTiles
   if not os.path.isdir('./work'):
      os.mkdir('./work')
   
if __name__ == "__main__":
    # Run the main routine
   main()
