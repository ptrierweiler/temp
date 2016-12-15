#!/usr/bin/python3
import os
import zipfile
from datetime import datetime, timedelta
import psycopg2
import argparse

parser = argparse.ArgumentParser(description='Download and ingest NCEP reanalysis temperture')

parser.add_argument('-y', type=str, metavar='2016', nargs='?', required=True,
                    help='Year of data')

args = parser.parse_args()

year = args.y

path = "/data/temp/ncep_reanal/"

link = "ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis/surface/air.sig995.{yr}.nc".format(yr=year)

# file test overwriting if exists
ncep_file = path + "air.sig995.{yr}.nc".format(yr=year)
if os.path.isfile(ncep_file) == True:
   os.remove(ncep_file)
    
# Downloading should be using python commands 
os.system('wget {link} -P {path}'.format(path=path, link=link))

# creating list of bands
os.system('gdalinfo -nomd NETCDF:"{ncep_file}":air > /scripts/temp/band.txt'.format(
ncep_file=ncep_file))

band_file= '/scripts/temp/band.txt'
with open(band_file, 'r', encoding='utf-8') as f:
    bands = f.readlines()
    
# finding max band
max_band = int(bands[-3].split()[1])
print(max_band)
 
try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()
    
conn.autocommit = True
cur = conn.cursor()

# extracting bands 
for i in range(1,max_band+1):
    p = i%4
    if p == 1:
       d = (i-1)/4
    if p == 0:
       p = 4
    
    tif_name = "ncep_temp_{yr}{doy}_{p}".format(yr=str(year), doy=str(int(d+1)).zfill(3),  p=p)
    name = path + tif_name
    print(name)
    db_date = datetime.strptime(tif_name.split('_')[2],'%Y%j').strftime('%Y%m%d')
    table = "ncep_temp_{date}_{p}".format(date=db_date,  p=tif_name.split('_')[-1])
    cur.execute("select count(*) from pg_catalog.pg_tables where schemaname = 'ncep_temp' and \
    tablename = '{tab}'".format(tab=table))
    tab_out = cur.fetchall()[0][0]    
    if tab_out == 0:    
        print("Extracting " + name)
        os.system('gdal_translate -b  {b} NETCDF:"air.sig995.{yr}.nc":air {out}.tif' .format(yr=str(year), b=i, out=name))
        print("Splitting " + name)
        os.system('gdal_translate -srcwin 0 0 72 73 -a_ullr 0 90 180 -90 {out}.tif {out}_east.tif'.format(out=name))
        os.system('gdal_translate -srcwin 72 0 72 73 -a_ullr -180 90 0 -90 {out}.tif {out}_west.tif'.format(out=name))
        print("Merging " + name.replace('.tif','_fix.tif'))
        os.system('gdal_merge.py -o {out}_fix.tif {out}_east.tif {out}_west.tif'.format(out=name))
        db_date = datetime.strptime(tif_name.split('_')[2],'%Y%j').strftime('%Y%m%d')
        print(db_date, p)
        os.system("gdalwarp -t_srs WGS84 {out}_fix.tif {out}_wgs84.tif".format(out=name))
        os.system("raster2pgsql -I   {out}_wgs84.tif -d ncep_temp.{tab} | psql tlaloc".format(out=name, tab=table))
        os.remove("{out}_east.tif".format(out=name))
        os.remove("{out}_west.tif".format(out=name))
        os.remove("{out}.tif".format(out=name))
        #os.remove("{out}_fix.tif".format(out=name))
        print("{out}.tif".format(out=name))
        # resample down .5
        cur.execute("update ncep_temp.{tab} set rast=ST_Rescale(rast, .5, -.5)".format(tab=table))
    else:
        print(table + " exists in the db")
   
cur.close()
conn.close()