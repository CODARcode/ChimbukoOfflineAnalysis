import plotly.express as px
import plotly.graph_objs as go
import plotly.io as pio
import numpy
import duckdb
from pypika import *
from ...provenance_database import *
pio.renderers.default = 'browser'

def toColor(val):
    return 'hsl(' + str(hash(val) % 360) + ', 50%, 50%)' 
def toSize(val):
    assert(val >= 0.0 and val <= 1.0)
    return 10 + 20*val
def getTableName(tab):
    if isinstance(tab, Table):
        return tab.get_sql()
    elif isinstance(tab, str):
        return tab
    else:
        assert 0

#Comparison colums are those columns for which separate columns exist for the two data sets
class ComparisonColumn:
    #norm: Normalize the data to range (0, 1). Required for the "size" column, optional otherwise
    #descr: Description of the column, used for hover text
    #col_A, col_B: column names for the two datasets
    def __init__(self, descr, col_A, col_B, norm = True):
        self.col_A = col_A
        self.col_B = col_B        
        self.norm = norm
        self.descr = descr
                       
    def getData(self,dtype,comb_table, con):
        adata = con("SELECT %s FROM %s" % (self.col_A, comb_table) ).fetchnumpy()[self.col_A]
        bdata = con("SELECT %s FROM %s" % (self.col_B, comb_table) ).fetchnumpy()[self.col_B]
        mina = numpy.min(adata)
        minb = numpy.min(bdata)
        maxa = numpy.max(adata)
        maxb = numpy.max(bdata)
        minv = mina if mina < minb else minb
        maxv = maxa if maxa > maxb else maxb

        rdata = numpy.concatenate((adata, bdata))
        uadata = adata
        ubdata = bdata
        if self.norm == True:
            uadata = (adata - minv)/(maxv - minv)
            ubdata = (bdata - minv)/(maxv - minv)            

        udata = numpy.concatenate((uadata, ubdata))
        print(udata)
        
        if dtype == "x":
            return numpy.concatenate((-uadata, ubdata)),  rdata
        elif dtype == "y":
            return udata, rdata
        elif dtype == "size":
            assert self.norm == True, "Size columns require normalization"
            return numpy.array([toSize(val) for val in udata]), rdata
        elif dtype == "color":
            return [toColor(val) for val in udata], rdata
        else:
            assert 0

    def getHoverDescription(self):
        return self.descr

#Label columns are columns shared between the two datasets, i.e those used for the table join
class LabelColumn:
    #descr: Description of the column, used for hover textdescr: Description of the column, used for hover text
    #col : The column name
    #post_trans:
    #  "hash" : Convert the entries to hashes via python hash
    #  "index" : Instead of values, simply replace the entries by numbered indices in the table order
    #  None : Show labels directly
    def __init__(self, descr, col, post_trans = None):
        self.col = col
        self.post_trans = post_trans
        self.descr = descr

    def getData(self,dtype,comb_table, con):
        rdata = con("SELECT %s FROM %s" % (self.col, comb_table) ).fetchnumpy()[self.col]        
        
        if(self.post_trans == "hash"):
            tdata = numpy.array([ hash(i) for i in rdata ])
        elif(self.post_trans == "index"):
            tdata = numpy.array([ i for i in range(len(rdata)) ] )
        else:
            tdata = rdata

        rdata_dup = numpy.concatenate((rdata,rdata))
        tdata_dup = numpy.concatenate((tdata,tdata))
        
        if dtype == "x" or dtype == "y":
            return tdata_dup,rdata_dup
        elif dtype == "color":
            return [toColor(val) for val in tdata_dup], rdata_dup
        else:
            assert 0
            
    def getHoverDescription(self):
        return self.descr


class LollipopChart:
    def __init__(self, pdb_con : ProvenanceDatabaseConnection, comb_table : Table):
        self.con = pdb_con
        self.table = getTableName(comb_table)

    def _create_data(self, xdata, ydata, color_data, size_data, hover_text):
        # reference: https://plotly.com/python/line-and-scatter/        
        return [
            go.Scatter(
                x=xdata,
                y=ydata,
                mode='markers',
                marker=dict(
                    color=color_data,
                    size=size_data,
                    sizemode='diameter',
                ),
                text=hover_text,
                hovertemplate='%{text}',
            )
        ]

    def _create_shapes(self, xdata, ydata):
        shapes = []
        for i in range(len(xdata)):
            shape = dict(
                    type='line',
                    xref='x',
                    yref='y',
                    x0=0,
                    y0=ydata[i],
                    x1=xdata[i],
                    y1=ydata[i],
                    line=dict(color='black', width=2)
                )
            shapes.append(shape)
        return shapes

    def _create_hover_text(self, xdata, ydata, color_data, size_data, xdescr, ydescr, color_descr, size_descr):
            return ["%s: %s<br>%s: %s<br>%s: %s<br>%s: %s" % 
                    (xdescr, xdata[i], ydescr, ydata[i], color_descr, color_data[i], size_descr, size_data[i]) for i in range(len(xdata))]

    #Generate the lollipop as a plotly graph objects Figure
    def create_lollipop(self, xcol, ycol, color_col, size_col, xaxis_label, yaxis_label):
        xdata, xdata_raw = xcol.getData("x", self.table, self.con)
        ydata, ydata_raw = ycol.getData("y", self.table, self.con)
        color_data, color_data_raw = color_col.getData("color", self.table, self.con)
        size_data, size_data_raw = size_col.getData("size", self.table, self.con)
        print("Sizes : ", len(xdata) , " " , len(ydata) , " " , len(color_data) , " " , len(size_data) )

        hover_text = self._create_hover_text(xdata_raw, ydata_raw, color_data_raw, size_data_raw, 
                                            xcol.getHoverDescription(), ycol.getHoverDescription(),
                                            color_col.getHoverDescription(), size_col.getHoverDescription() )
        plot_data = self._create_data(xdata, ydata, color_data, size_data, hover_text)
        shapes = self._create_shapes(xdata,ydata)
        
        
        layout = go.Layout(
            shapes=shapes,
            width=1000,
            height=1000,
        )

        fig = go.Figure(plot_data, layout)

        #Add vertical line marking 0 on x axis
        fig.add_shape(
            type='line',
            x0=0,
            y0=numpy.min(ydata),
            x1=0,
            y1=numpy.max(ydata),
            line=dict(color='black', width=2)
        )
        fig.update_xaxes(title_text=xaxis_label)
        fig.update_yaxes(title_text=yaxis_label)
        
        return fig

    #Create and show the lollipop chart; this is the default interface
    def show(self, x_col, y_col, color_col, size_col, xaxis_label, yaxis_label):
        fig = self.create_lollipop(x_col, y_col, color_col, size_col, xaxis_label, yaxis_label)
        pio.show(fig)    


#Show a lollipop chart comparing the call stack summaries
def CallStackSummariesComparison(pdb: ProvenanceDatabase, pdb2: ProvenanceDatabase):
    assert pdb.pdb_con is pdb2.pdb_con
    con = pdb.pdb_con

    #Build combined table from call stack summaries
    pdb.getCallStackSummaries(0).to_view("__CallStackSummariesComparison__tmp1")
    pdb2.getCallStackSummaries(0).to_view("__CallStackSummariesComparison__tmp2")
    r2 = Table("__CallStackSummariesComparison__tmp1")
    r3 = Table("__CallStackSummariesComparison__tmp2")
    con(Query.from_(r2).select(r2.call_stack_label, r2.fname, r2.fname_hash, r2.anomaly_count.as_("anomaly_count_A"), r3.anomaly_count.as_("anomaly_count_B"), r2.avg_severity.as_("avg_severity_A"),  r3.avg_severity.as_("avg_severity_B")  )
    .inner_join(r3).on( (r3.call_stack_label == r2.call_stack_label) & (r3.fname_hash == r2.fname_hash) )).to_view("__CallStackSummariesComparison__tmp3")
    con("SELECT * FROM __CallStackSummariesComparison__tmp3 ORDER BY avg_severity_A + avg_severity_B DESC LIMIT 10").to_view("__CallStackSummariesComparison__tmp4")

    #Generate chart
    chart = LollipopChart(con, "__CallStackSummariesComparison__tmp4")
    xcol = ComparisonColumn("Anomaly count","anomaly_count_A","anomaly_count_B",True)
    ycol = LabelColumn("Call stack label", "call_stack_label",post_trans="index")
    color_col = LabelColumn("Function name","fname",post_trans="hash")
    size_col = ComparisonColumn("Severity","avg_severity_A","avg_severity_B",True)

    chart.show(xcol,ycol,color_col,size_col,"Normalized anomaly count","Call stack index")
