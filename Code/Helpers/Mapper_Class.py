import os
import geopandas as gpd
import pandas as pd
import folium
import numpy as np

class Mapper:
    def __init__(self, shp=None, data=None):
        self.shp = shp
        self.data = data

    def getGDF(self):
        print(os.getcwd())
        shp = gpd.read_file("Data/Virginia2020/Virginia.shp").set_crs("EPSG:4326")
        data = pd.read_csv("Data/Final_Data/trainableData.csv") 
        gdf = gpd.GeoDataFrame(data.merge(shp[['Tmc', 'TmcType', 'RoadNumber', 'IsPrimary', 'geometry']], left_on=['TMC'], right_on=['Tmc'], how='left'))
        return gdf

    def coords(self, geom):
        return [(point[1],point[0]) for point in geom.coords]

    def quickMap(SELF, gdf, col):
        if pd.api.types.is_numeric_dtype(gdf[col]):
            vmin = gdf[col].quantile(.1)
            vmax = gdf[col].quantile(.9)
            return gdf.explore(col, tiles='cartoDB positron', cmap='Reds', vmin=0, vmax=vmax)
        else:
            return gdf.explore(col, tiles='cartoDB positron', cmap='bwr')

    def makePeriodByYearMap(self, period, year):
        gdf = self.getGDF()
        gdf['points'] = gdf.apply(lambda row: self.coords(row.geometry), axis=1)
        periodGDF = gdf.loc[gdf.PERIOD == period]
        obsmap = self.quickMap(periodGDF.loc[periodGDF.YEAR==year].drop('points', axis=1), 'obs rel_unrel')
        return obsmap

    
