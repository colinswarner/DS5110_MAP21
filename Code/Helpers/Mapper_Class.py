import os
import geopandas as gpd
import pandas as pd
import folium
import numpy as np
import ipywidgets as widgets
from IPython.display import display, clear_output, Markdown

class Mapper:
    def __init__(self, shp=None, data=None, maps={}, years=None, periods=None, variables=None):
        self.shp = shp
        self.data = data
        self.maps = maps
        self.years = years
        self.periods = periods
        self.variables = variables

    def getGDF(self):
        print(os.getcwd())
        shp = gpd.read_file("Data/Virginia2020/Virginia.shp").set_crs("EPSG:4326")
        data = pd.read_csv("Data/Final_Data/trainableData.csv") 
        gdf = gpd.GeoDataFrame(data.merge(shp[['Tmc', 'TmcType', 'RoadNumber', 'IsPrimary', 'geometry']], left_on=['TMC'], right_on=['Tmc'], how='left'))
        return gdf

    def coords(self, geom):
        return [(point[1],point[0]) for point in geom.coords]

    def quickMap(self, gdf, col):
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
    
    def createMapViews(self, gdf, _variables):
        display(Markdown(f"### Creating Map Views"))
        self.years = gdf.YEAR.unique()
        print(f"Possible Years: {self.years}")
        self.periods = gdf.PERIOD.unique()
        print(f"Possible Periods: {self.periods}")
        self.variables = _variables
        print(f"Possible Variables: {self.variables}")
        print(f"Creating Views:")
        for year in self.years:
            self.maps[year] = {}
            for period in self.periods:
                self.maps[year][period] = {}
                for variable in self.variables:
                    try:
                        temp_df = gdf.loc[(gdf.YEAR == year) & (gdf.PERIOD == period)][['TMC',variable,'geometry']]
                        self.maps[year][period][variable] = mapper.quickMap(temp_df, variable)
                        print(f"- Created: {year}, {period}, {variable}")
                    except:
                        print(f"- Failed: {year}, {period}, {variable}")
                        
    def addMapViews(self, gdf, variables):
        for variable in variables:
            self.variables.append(variable)
        display(Markdown(f"### Adding Map Views"))
        print(f"Additional Variables: {variables}")
        print(f"Creating Views:")
        for year in self.years:
            for period in self.periods:
                for variable in variables:
                    try:
                        temp_df = gdf.loc[(gdf.YEAR == year) & (gdf.PERIOD == period)][['TMC',variable,'geometry']]
                        self.maps[year][period][variable] = mapper.quickMap(temp_df, variable)
                        print(f"- Created: {year}, {period}, {variable}")
                    except:
                        print(f"- Failed: {year}, {period}, {variable}")
                        
    def viewMaps(self):
        @widgets.interact(year=self.years, period=self.periods, variable=self.variables)
        def changeMap(year, period, variable):
            return mapper.maps[year][period][variable]
    
