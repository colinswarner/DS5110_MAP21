import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import geopandas as gpd
import folium

class Visualizer:
    def __init__(self,df):
        self.df = df

    def makeHistSubplots(self, cols, title):
        fig = make_subplots(rows=2, cols=3)
        fig.add_trace(go.Histogram(x=self.df[cols[0]], name=cols[0]), row=1, col=1)
        fig.add_trace(go.Histogram(x=np.log(self.df[cols[0]]), name="log("+cols[0]+")"), row=2, col=1)
        fig.add_trace(go.Histogram(x=self.df[cols[1]], name=cols[1]), row=1, col=2)
        fig.add_trace(go.Histogram(x=np.log(self.df[cols[1]]), name="log("+cols[1]+")"), row=2, col=2)
        fig.add_trace(go.Histogram(x=self.df[cols[2]], name=cols[2]), row=1, col=3)
        fig.add_trace(go.Histogram(x=np.log(self.df[cols[2]]), name="log("+cols[2]+")"), row=2, col=3)
        fig.update_layout(height=800, width=1200, title_text=title)
        fig.show()

    def makeCatBoxPlots(self, x, y, title):
        fig = px.box(self.df, x=x, y=y, color="obs rel_unrel",
             notched=True, # used notched shape
             title=title,
            )
        fig.show()