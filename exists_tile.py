#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Fetch Menu Data from Database
# server.py

import os
from flask import Flask,request,g
import sqlite3
import json
import math
from flask_cors import CORS

application = Flask(__name__)
cors = CORS(application)
DATABASE = './detail.mbtiles'
bounds = {}

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
       db = g._database = sqlite3.connect(DATABASE)
       db.row_factory = sqlite3.Row
       print("opening Database")
    return db

@application.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@application.route('/')
def exists():
    global bounds
    cur = get_db().cursor()

    sql = 'select value from metadata where name = "bounds"';
    cur.execute(sql)
    row = cur.fetchone()
    if row:
       value = row['value']
       value = value.split(',')
       bounds['xmin'] = float(value[0])
       bounds['ymin'] = float(value[1])
       bounds['xmax'] = float(value[2])
       bounds['ymax'] = float(value[3])
       #print('xmin:%f xmax:%f ymin:%f ymax:%f'%\
       #     (bounds['xmin'],bounds['xmax'],bounds['ymin'],bounds['ymax'],))

       lonrequest = float(request.args.get('lon','-122'))
       latrequest = float(request.args.get('lat','37.14'))
       zoomrequest = float(request.args.get('zoom','6')) + 1
       tileX,tileY = deg2num(latrequest,lonrequest,zoomrequest)
       zoom = int(zoomrequest)
       tileY = (2 ** zoom) - tileY - 1
       sql = 'select * from tiles where zoom_level = %s and tile_column = %s and tile_row = %s'%(zoom,tileX,tileY,)
       cur.execute(sql)
       rv = cur.fetchone()
       outstr = ''
       outstr += '{"success":'
       if rv:
          outstr += '"true"}'
       else:
          #print('x:%s y:%s'%(lonrequest,latrequest,))
          if zoom > 14 and \
                 lonrequest >= bounds['xmin'] and \
                 lonrequest <= bounds['xmax'] and \
                 latrequest >= bounds['ymin'] and \
                 latrequest <= bounds['ymax'] :
              outstr += '"true"'
          else:
              outstr += 'false}'
       print outstr
       return outstr
    else:
      return "Mbtiles metadata for bounds is missing -> limits to zoom 14"

@application.route('/summary')
def summary():
    cur = get_db().cursor()
    sql = 'select zoom_level, min(tile_column),max(tile_column),min(tile_row),max(tile_row), count(zoom_level) from tiles group by zoom_level;'
    cur.execute(sql)
    rows = cur.fetchall()
    outstr ='Zoom Levels Found:%s'%len(rows) + '<br>'
    for row in rows:
        #print(row)
        for member in row:
            outstr += str(member) + '\t'
        outstr += '<br>'
    return outstr

def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  return (xtile, ytile)
    
if __name__ == "__main__":
    application.run(host='0.0.0.0',port=9458)

#vim: tabstop=3 expandtab shiftwidth=3 softtabstop=3 background=dark
