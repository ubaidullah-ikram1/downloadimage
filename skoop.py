# -*- coding: utf-8 -*-
"""
@author: ubaidullah ikram
"""
import geopandas
import pandas as pd
import requests
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import create_engine
from sentinelsat import SentinelAPI
import psycopg2  
import json
# define the connection string for the postgresql database
db_connection_url = "postgresql://postgres:1234@localhost:5432/downloadsatelite"
con = create_engine(db_connection_url)
# query for getting farm ids and geometries 
sql = "select geometry from skoopinsert"
# store the result from the query directly into a geodataframe
geo = geopandas.GeoDataFrame.from_postgis(sql, con, crs=4326, geom_col='geometry') 
print(geo.loc[0]['geometry'])
# sentinel hub credentials
user = 'hasan.mustafa'
password = 'neverlock123'
# folder to store the downloaded images
datafolder = '../IMAGES/'
api = SentinelAPI(user, password, 'https://apihub.copernicus.eu/apihub')
#api = SentinelAPI(user, password, 'https://scihub.copernicus.eu/dhus')
# geodataframe variable
# search by polygon, time, and SciHub query keywords
# footprint = geojson_to_wkt(read_geojson(boundingBox))
#print(len(geo))
#footprint = boundingBox[0]
products = api.query(geo.loc[0]['geometry'],
                    date=('20220605','20220705'),
                    platformname='Sentinel-2',
                    producttype='S2MSI2A',
                    cloudcoverpercentage=(0, 30))
                    #limit=4)
# GeoJSON FeatureCollection containing footprints and metadata of the scenes
# api.to_geojson(products)
# GeoPandas GeoDataFrame with the metadata of the scenes and the footprints as geometries
#print(api.to_geodataframe(products))
geodataframe = api.to_geodataframe(products)
print(geodataframe)

for index, row in geo.iterrows():
    #print(row['geometry'])
    products = api.query(row['geometry'],
                        date=('20220605','20220705'),
                        platformname='Sentinel-2',
                        producttype='S2MSI2A',
                        cloudcoverpercentage=(0, 30))
                        #limit=4)
    print(len(products))
    
    geodataframe = geodataframe.append(api.to_geodataframe(products))
    geodataframe.set_crs(4326, allow_override=True)
print(geodataframe)
print(len(geodataframe))
geodataframe.to_postgis('sentinel2_data', con, if_exists='append', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})
# my_query = 'DELETE FROM sentinel2_data WHERE id_serial IN (SELECT id_serial FROM (SELECT id_serial, ROW_NUMBER() OVER( PARTITION BY uuid ORDER BY id_serial ) AS row_num FROM sentinel2_data ) t WHERE t.row_num > 1 );'
# del_query = 'delete from sentinel2_data where title not in(select s.title from sentinel2_data s inner join hbl_farms f on st_intersects(s.geom, f.geom))'
# con.execute(my_query)
# con.execute(del_query)
# read the sentinel2_data table and store its contents in dataframe, to be used for downloading the images
s2data = pd.read_sql_query('select * from sentinel2_data',con=con)
print(len(s2data))
for i in s2data.index:
    if   s2data.at[i, "downloaded"] =='no' :
        api.download(s2data.at[i, "uuid"], directory_path = datafolder )
        query2 = "update sentinel2_data set downloaded = 'yes' where uuid = '{}';".format(s2data.at[i, "uuid"])
        print(query2)
        con.execute(query2)     
    else:
        print('already downloaded, skipping uuid: ', s2data.at[i, "uuid"] )