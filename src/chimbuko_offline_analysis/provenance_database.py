import duckdb
import pypika
from pypika import *
import numpy

class ProvenanceDatabaseConnection:
    def __init__(self):
        self.con = duckdb.connect()
        self.db = []
    def connect(self, file : str):
        ndb = len(self.db)
        db_nm = "pdb_%d" % ndb
        self.con.sql("ATTACH '%s' AS %s (READ_ONLY)" % (file,db_nm))
        self.db.append(ProvenanceDatabase(self, db_nm))
        return self.db[-1]
        
    def __call__(self, query : pypika.queries.QueryBuilder) -> duckdb.duckdb.DuckDBPyRelation:
        if type(query) == pypika.queries.QueryBuilder:
            return self.con.sql(query.get_sql()) #quote_char=None
        elif type(query) == pypika.queries._SetOperation:
            return self.con.sql(query.get_sql())
        elif type(query) == str:
            return self.con.sql(query)    
        else:
            raise Exception("Invalid input type")

class ProvenanceDatabase:
    def __init__(self, pdb_con, db_nm):
        #auto-generate the table objects
        self.db_nm = db_nm
        self.pdb = Database(db_nm)
        self.pdb_con = pdb_con
        print("Tables:")
        for n in pdb_con("SELECT DISTINCT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'").fetchnumpy()["table_name"]:
            exec("self.%s = Table(\"%s\", self.pdb)" % (n,n))
            print(n)        

    def __call__(self, query : pypika.queries.QueryBuilder) -> duckdb.duckdb.DuckDBPyRelation:
        return self.pdb_con(query)

    def describe(self, table : str):
        q = ("DESCRIBE %s." % self.db_nm) + table
        return self.__call__(q)
    def listTables(self):        
        return self("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_catalog='%s'" % self.db_nm)
    def listColumns(self, table : Table):
        return self("SELECT COLUMN_NAME FROM duckdb_columns() WHERE TABLE_NAME = '%s' AND database_name='%s'" % (table.get_table_name(),self.db_nm) )
    def listColumnsAsArray(self, table: Table):
        return self.listColumns(table).fetchnumpy()["column_name"]

    def getFunctionName(self, fid):
        f = self.functions
        return self(Query.from_(f).select(f.name).where(f.fid == fid) ).fetchnumpy()["name"][0]

    #Get the number of anomalies recorded for the given function idx    
    def getFunctionAnomalyCount(self, fid):
        d=self.func_anomaly_count_stats
        return self(Query.from_(d).select(d.accumulate).where(d.fid==fid)).fetchnumpy()['accumulate'][0]

    #Get the number of executions observed for the given function idx
    def getFunctionExecutionCount(self, fid):    
        d=self.func_runtime_profile_exclusive_stats
        return self(Query.from_(d).select(d.count).where(d.fid==fid)).fetchnumpy()['count'][0]

    #Get the AD model histogram of exclusive execution times for the given function (HBOS/COPOD only)
    #Return ( [edges], [counts] )  where [edges] includes the left edge of the first bin and the right edge of the last bin
    #The output can be plotted using matplotlib.pyplot.hist(edges[:-1], edges, weights=counts)
    def getFunctionADmodelHistogram(self, fid):
        d = self.ad_models
        ts = self(Query.from_(d).select(d.bin_width, d.first_edge, d.bin_counts).where(d.fid==fid) ).fetchnumpy()
        nbin=len(ts['bin_counts'][0])
        edges = [ ts['first_edge'][0] + b*ts['bin_width'][0] for b in range(0,nbin+1) ]
        counts = ts['bin_counts'][0]
        return edges, counts

    #Get the times in seconds (defined by the function *exit* event) of anomalies on a specific pid/rank, with optional 
    #specification of function idx
    #Results can be plotted using a histogram, e.g. matplotlib.pyplot.hist(times, bins=100)    
    def getAnomalyTimes(self, pid, rid, fid=None):
        d = self.anomalies
        times = None
        if fid is None:            
            times = self(Query.from_(d).select(d.exit).where( (d.pid==pid) & (d.rid==rid) )).fetchnumpy()['exit']
        else:            
            times = self(Query.from_(d).select(d.exit).where( (d.pid==pid) & (d.rid==rid) & (d.fid==fid) )  ).fetchnumpy()['exit']        
        times = (times - self.getRunStartTime(pid,rid))/1e6
        return times
    
    def getEventExecWindow(self, event_id : str):
        ew = self.exec_windows
        ewe = self.exec_window_events
        return self(Query.from_(ew).select(ewe.star).where(ew.event_id == event_id )
            .inner_join(ewe).on( ew.exec_window_entry_id == ewe.event_id ) )

    def getEventCallStack(self, event_id : str):
        cs = self.call_stacks
        cse = self.call_stack_events
        return self(Query.from_(cs).select(cs.entry_idx, cse.star).where(cs.event_id == event_id )
                    .inner_join(self.call_stack_events).on( cs.call_stack_entry_id == cse.event_id )
                    .orderby(cs.entry_idx, order=Order.desc)   
                    )

    #Get the information on the call stack with the provided label/hash
    def getLabeledCallStack(self, label):
        d=self.call_stack_labels
        eids = self(Query.from_(d).select(d.star).where(d.call_stack_label == label)).fetchnumpy()['event_id']
        if(len(eids) == 0):
            raise Exception("Could not find the provided label in the map")
        eid=eids[0]
        r = self.getEventCallStack(eid).to_view("r")
        rt = Table("r")
        func = self.functions
        return self(Query.from_(rt).select(rt.entry_idx,rt.fid,func.name).inner_join(func).on(rt.fid == func.fid).orderby(rt.entry_idx, order=Order.desc)   )

    #Return a dictionary of (call stack label)-> count for events in the provided subset ("anomalies","normal_execs","both")
    def getFunctionCallStackLabelsAndCounts(self, fid, subset = 'anomalies'):
        anom = self.anomalies
        lb = self.call_stack_labels
        normal = self.normal_execs
        q = None
        if subset == 'anomalies':
            q = Query.from_(anom).select(lb.call_stack_label).inner_join(lb).on(anom.event_id == lb.event_id).where(anom.fid == fid)
        elif subset == 'normal_execs':
            q = Query.from_(normal).select(normal.event_id, lb.call_stack_label).inner_join(lb).on(normal.event_id == lb.event_id).where(normal.fid == fid)
        elif subset == 'both':
            q = Query.from_(anom).select(anom.event_id, lb.call_stack_label).inner_join(lb).on(anom.event_id == lb.event_id).where(anom.fid == fid) + Query.from_(normal).select(normal.event_id, lb.call_stack_label).inner_join(lb).on(normal.event_id == lb.event_id).where(normal.fid == fid)
        else:
            raise Exception("Invalid subset")
        
        labels = self(q).fetchnumpy()['call_stack_label']
        ulabels = dict()
        for l in labels:
            if l not in ulabels.keys():
                ulabels[l] = 1
            else:
                ulabels[l] += 1
        return ulabels
    

    
    #Tabulate summary information on functions, sorted in descending order by accumulated severity (order_by="anom_severity") or anomaly count (order_by="anom_count")
    def topFunctions(self, order_by = 'anom_severity'):
        sev = self.func_anomaly_severity_stats
        func = self.functions
        acnt = self.func_anomaly_count_stats
        ecnt = self.func_runtime_profile_exclusive_stats

        ob = None
        if order_by == 'anom_severity':
            ob = sev.accumulate
        elif order_by == 'anom_count':
            ob = acnt.accumulate
        else:
            raise Exception("Unsupported sort order")
                    
        return self(Query.from_(sev).select(sev.accumulate.as_("accum_sev"), sev.fid, func.pid, acnt.accumulate.as_("anomalies"), ecnt.count.as_("calls"), func.name )
            .inner_join(func).on(func.fid == sev.fid)
            .inner_join(acnt).on(func.fid == acnt.fid)
            .inner_join(ecnt).on(func.fid == ecnt.fid)
            .orderby(ob, order=Order.desc))
        
    def getRunStartTime(self, pid, rank):
        d = self.io_steps
        return self(Query.from_(d).select(d.io_step_tstart)
            .where( (d.pid == pid) & (d.rid == rank) & (d.io_step) == 0 ) ).fetchnumpy()['io_step_tstart'][0]

    #Return the primary table (anomalies / normal_execs) for the specific event
    def getEventPrimaryTable(self, event_id):
        d=self.anomalies
        if(len(
                self( Query.from_(d).select(d.event_id).where(d.event_id == event_id)).fetchnumpy()['event_id'] 
        ) != 0):
            return d
        d=self.normal_execs
        if(len(
                self( Query.from_(d).select(d.event_id).where(d.event_id == event_id)).fetchnumpy()['event_id'] 
        ) != 0):
            return d
        raise Exception("Could not find event ",event_id)

    #Return the node memory status recorded at a timestamp as close as possible to the function execution timestamp
    def getEventNodeMemoryStatus(self, event_id):
        prim=self.getEventPrimaryTable(event_id)
        ns=self.node_state
        nn=self.rank_node_map
        r = self(Query.from_(prim).select(prim.entry,prim.exit,prim.pid,prim.rid,ns.star,nn.hostname)
                 .inner_join(ns).on(ns.event_id == prim.event_id).where(prim.event_id == event_id)
                 .inner_join(nn).on( (nn.pid == prim.pid) & (nn.rid == prim.rid) )
                 ).to_view("r")

        #Convert the times into seconds since start
        rt = Table("r")
        rid = r.fetchnumpy()['rid'][0]
        pid = r.fetchnumpy()['pid'][0]
        
        start = self.getRunStartTime(rid,pid)
        s = self(Query.from_(rt).select( ( (rt.entry - start)/1e6 ).as_("entry_s") , ( (rt.exit - start)/1e6 ).as_("exit_s"),  ( (rt.timestamp - start)/1e6 ).as_("state_timestamp_s"), rt.star    )).to_view("s")
        return self('SELECT "pid", "rid", "hostname", "state_timestamp_s", "entry_s", "exit_s", "meminfo:MemFree (MB)" AS "free_MB",  "meminfo:MemTotal (MB)" AS "total_MB", "Memory Footprint (VmRSS) (KB)" AS "RSS_KB", "Heap Memory Used (KB)" AS "heap_memory_used_KB" FROM "s"')

    #Produce a table for a specific process pid containing:
    #- the call stack label
    #- a count of anomalies with that call stack
    #- the average severity for those anomalies
    #- the function name and a corresponding hash
    #
    #The output is sorted according to the average severity in descending order
    def getCallStackSummaries(self, pid):
        d = self.anomalies
        cl = self.call_stack_labels
        f = self.functions
        self(Query.from_(d).select(d.outlier_severity, cl.call_stack_label, f.name).where(d.pid==0).inner_join(cl).on(d.event_id == cl.event_id).inner_join(f).on(d.fid == f.fid) ).to_view("r")
        return self("SELECT call_stack_label, count(*) as anomaly_count, AVG(outlier_severity) as avg_severity, first(name) as fname, hash(first(name)) as fname_hash, FROM r GROUP BY call_stack_label ORDER BY avg_severity DESC")
    
            

