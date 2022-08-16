

from operator import invert
import os
import fiona
import rasterio
from rasterio.mask import mask
from rasterio.plot import show
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import create_engine
from sentinelsat import SentinelAPI
import geopandas
import pandas as pd
import json
import numpy as np
from sqlalchemy.sql import text
from zipfile import ZipFile
from rasterio.warp import calculate_default_transform, reproject, Resampling
# import rasterio
# from rasterio.warp import calculate_default_transform, reproject, Resampling
import glob
import pathlib


db_connection_url = "postgresql://postgres:1234@localhost:5432/downloadsatelite"
con = create_engine(db_connection_url)
# query for getting farm ids and geometries 
sql = "select geometry from skoopinsert"
# store the result from the query directly into a geodataframe
geo = geopandas.GeoDataFrame.from_postgis(sql, con, crs=4326, geom_col='geometry') 
for name in glob.glob('../reproject\*'):
    # function to return the file extension
    file_extension = pathlib.Path(name).suffix
   
    if(file_extension=='.tif'):
        
        rasterBand = rasterio.open(name)
        print(rasterBand)
        for index, row in geo.iterrows():
            out_image, out_transform = mask(rasterBand, [geo.loc[0]['geometry']], crop=True)
            # outMeta = rasterBand.meta  
            # outMeta.update({"driver": "GTiff",
            #             "height": out_image.shape[1],
            #             "width": out_image.shape[2],
            #             "transform": out_transform})
            # filename for the clipped ndvi file
            outpath='../clipdata/'
            #print(outpath)
            outname=outpath+'dasd'+'.tif'
            print(outname)
            # con.execute("INSERT INTO clipimages( image_path) VALUES ()")
            con.execute(text('INSERT INTO clipimages( image_path) VALUES (:group)'), group ='C:/Users/Ubaidullah Ikram/Documents/downloadimage_script/clipdata/'+outname)
            # use rasterio to save the clipped ndvi file
            with rasterio.open(outname, "w", **outMeta) as dest:
                dest.write(out_image)


    

    