import plotly.express as px
import plotly.graph_objs as go
import plotly.io as pio
import numpy
import duckdb
from pypika import *
from ...provenance_database import *
pio.renderers.default = 'browser'

class ParcoordsChart:
    def __init__(self, table : Table, pdb):
        if isinstance(table, Table):
            self.table = table.get_sql()
        elif isinstance(table, str):
            self.table = table
        else:
            assert 0
        self.pdb = pdb

    def _stripQuotes(self, string):
        return string.strip("\"'")
    
    def create_parcoords_plot(self, list_of_param, updated_labels, color_col):
        query = "FROM " + self.table + " SELECT"
        for i, par in enumerate(list_of_param):
            query += " " + par 
            if i != len(list_of_param)-1:
                query += ","
        df_pars = self.pdb(query).fetchnumpy()            
        print(df_pars)
        
        dimensions = []
        for i, par in enumerate(list_of_param):
            dimension = dict(label=updated_labels[i], values=df_pars[self._stripQuotes(par)])
            dimensions.append(dimension)

        color_col_s = self._stripQuotes(color_col)
        fig = go.Figure(data=go.Parcoords(
            line=dict(color=df_pars[color_col_s],
                      colorscale='Bluered',
                      showscale=True,
                      cmax=numpy.max(df_pars[color_col_s]),
                      cmin=numpy.min(df_pars[color_col_s])),
            dimensions=dimensions
        ))
        

        return fig

    def show(self, list_of_param, updated_labels, color_col, font_size=25, tick_font_size=18, height=700):
        fig = self.create_parcoords_plot(list_of_param, updated_labels, color_col)
        
        # Set font size for the tick labels
        fig.update_layout(font=dict(size=font_size))
        fig.update_traces(tickfont_size=tick_font_size, selector=dict(type='parcoords'))
        fig.update_layout(height=height)
        
        pio.show(fig)

#Produce a parallel coordinates plot of some important variables for the top 'topn' anomalies by severity
def AnomalySummary(pdb, topn=10):
    #Generate a table with the required information
    d = pdb.anomalies
    nn = pdb.node_state
    pdb(Query.from_(d).select(d.star,nn.star).inner_join(nn).on(d.event_id == nn.event_id).orderby('outlier_severity', order=Order.desc).limit(topn)).to_view("__AnomalySummary_tmp1")
    pdb.convertColumnToSecondsSinceStart("__AnomalySummary_tmp1","entry","entry_s").to_view("__AnomalySummary_tmp2")
    pdb("FROM __AnomalySummary_tmp2 SELECT *, \"Memory Footprint (VmRSS) (KB)\"/1024 AS RSS_MB").to_view("__AnomalySummary_tmp3")

    #Generate the chart
    par = ParcoordsChart("__AnomalySummary_tmp3",pdb)
    par.show(['entry_s', 'RSS_MB', '"meminfo:MemFree (MB)"', 'outlier_severity'],['Entry time (s)', 'Resident set size (MB)', 'Mem free (MB)', 'Severity'], 'outlier_severity')
