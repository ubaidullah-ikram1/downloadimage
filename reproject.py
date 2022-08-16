
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
for name in glob.glob('../IMAGES\*'):
    # print(name+'/S2B_MSIL2A_20220624T054639_N0400_R048_T43SCS_20220624T082113.SAFE/GRANULE/L2A_T43SCS_A027673_20220624T055601/IMG_DATA/R10m')
    with ZipFile(name, 'r') as zipObj:
        zipObj.extractall('../unzip')
       
src_crs = {'init': 'EPSG:4326'}   
for name in glob.glob('../unzip\*'):
    path=name+'/GRANULE/'
    dir_list = os.listdir(path)
    paths=name+'/GRANULE/'+dir_list[0] + '/IMG_DATA/R10m/'
    img=os.listdir(paths)
    print(img[5])
    rasterPath = os.path.join(paths,img[5])
    rasterBand = rasterio.open(rasterPath)
    transform, width, height = calculate_default_transform(
    rasterBand.crs, src_crs, rasterBand.width, rasterBand.height, *rasterBand.bounds)
    kwargs = rasterBand.meta.copy()
    kwargs.update({
        'crs': src_crs,
        'transform': transform,
        'width': width,
        'height': height
    })
    print(kwargs)
    with rasterio.open('../reproject/'+img[5]+'.tif', 'w', **kwargs) as dst:
        for i in range(1, rasterBand.count + 1):
            reproject(
                source=rasterio.band(rasterBand, i),
                destination=rasterio.band(dst, i),
                src_transform=rasterBand.transform,
                src_crs=rasterBand.crs,
                dst_transform=transform,
                dst_crs=src_crs,
                resampling=Resampling.nearest)
        
        

    

    
    
    
      
        
   


    




            
                 








 
    

               

        
               