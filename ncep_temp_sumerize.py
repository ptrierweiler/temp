#!/usr/bin/python3
import psycopg2
from datetime import datetime, timedelta
import argparse


schema = 'ncep_temp'
table = 'data'

try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()

conn.autocommit = True    
cur = conn.cursor()

def summerize(layer,yr):
    # Find dates in db
    print("Finding dates in {schema}.{table} for {layer}".format(schema=schema, 
    table=table, layer=layer))
    cur.execute("select distinct date::text from {schema}.{table} where \
    geolayer = '{layer}' and EXTRACT(YEAR FROM date) = {yr}".format(schema=schema, 
    table=table, layer=layer, yr=int(yr)))
    db_list = cur.fetchall()
    
    db_list_cln = []
    for i in db_list:
	      db_list_cln.append(i[0])
        
        
    # image list
    cur.execute("select tablename from pg_tables \
    where schemaname = '{sch}' and tablename <> 'data'\
    and tablename ~ '_{yr}...._[1234]'".format(sch=schema, yr=yr))
    image_list_db = cur.fetchall()
    date_list = []
    for i in image_list_db:
        n = i[0]
        yr = n.split('_')[2][0:4]
        mn = n.split('_')[2][4:6]
        dy = n.split('_')[2][6:8]
        date = "{yr}-{mn}-{dy}".format(yr=yr, mn=mn, dy=dy)
        date_list.append(date)
    
    date_diff_list = list(set(date_list) - set(db_list_cln))
  
  
  	# summerizing files
    for i in date_diff_list:
        print("processing date: " + i)
        base = "{schema}.ncep_temp_{date}".format(schema=schema, date=i.replace('-',''))
        geo = "wgs84" '.' + layer
        pydate = datetime.strptime(base.split('_')[-1],'%Y%m%d').date()
        db_date = pydate.strftime('%Y-%m-%d')
        data_list = []
        for i in range(4):
            ln = i+1
            image = base + '_'+ str(ln)
            print(image)
            cur.execute("SELECT gid, (stats).count,(stats).mean::numeric(7,3), \
            median::numeric(7,3) FROM (SELECT gid, \
            ST_SummaryStats(ST_Clip(rast, {geo}.wkb_geometry,0)::raster) as stats, \
            ST_Quantile(ST_Clip(rast, {geo}.wkb_geometry,0)::raster,.5) as median \
            from {image}, {geo} where \
            st_intersects(rast, {geo}.wkb_geometry)) as foo".format(image=image, geo=geo))
            sum_data = cur.fetchall()
            data_list.append(sum_data)
  
        t1 = data_list[0]
        t2 = data_list[1]
        t3 = data_list[2]
        t4 = data_list[3]
      
        temp_dict = {}
        for i in t1:
            tmp_dict = {i[0]: [i[2]]}
            temp_dict.update(tmp_dict)
      
        for i in [t2, t3, t4]:
            for i2 in i:
                temp_dict[i2[0]].append(i2[2])
              
        for i in temp_dict:
            geo = i 
            row = temp_dict[geo]
            if row[0] != None:
                lt = float(min(row)) - 273.15
                ht = float(max(row)) - 273.15
                geo_layer = layer
                # testing if row exists
                cur.execute("select count(*) from {sch}.{tab} \
                where gid = '{geo}' and date = '{date}'".format(sch=schema,
                tab=table, geo=geo, date=db_date))
                cnt = cur.fetchall()[0][0]
                if cnt == 0:
                    cur.execute("insert into {sch}.{tab} values ('{geo}', '{date}', {mint}, \
                    {maxt}, '{ly}')".format(sch=schema, tab=table, date=db_date, geo=geo,
                    mint=lt, maxt=ht, ly=geo_layer))
                
summerize('brasil_mesoregion',2016)
summerize('ana_bacias', 2016)
summerize('nass_asds',2016)
cur.close()
conn.close()