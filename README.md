A Python-based library for interacting with Chimbuko's Provenance Database after conversion to a relational database. It offers advanced query and visualization functionality, enabling a detailed analysis and comparison of performance anomalies captured by Chimbuko.

# Installation

- Clone the repo into the current directory (using pip)
- `cd ChimbukoOfflineAnalysis`
- `pip install .`

# Usage

1. After running your application with the [Chimbuko performance analysis tool](https://github.com/CODARcode/Chimbuko), convert the UnQlite provenance database to DuckDB format using [ChimbukoProvDBconvert](https://github.com/CODARcode/ChimbukoProvDBconvert)
2. From Python
   ```
   import chimbuko_offline_analysis as chim
   con = ProvenanceDatabaseConnection()   #create a database connection
   pdb = con.connect("/path/to/your/duckdb/database") #connect to the database
   ```
   
   You can also connect to multiple databases by making repeated calls to `con.connect("/some/other/database")` 
   
## Analysis functionality

### Profiling

Profile information can be obtained using both the 'inclusive' (timing includes child function calls) and 'exclusive' (timing comprises only time spent within the function but not within child calls). For an application profile:
```
pdb.getApplicationProfile("inclusive")
pdb.getApplicationProfile("exclusive")
```
The profiles are sorted in descending order by the runtime. Here the exclusive profile also contains information on the anomaly **severity** (total amount of time spent in anomalous function executions) and fraction of time spent in anomalous function executions, providing a means of identifying functions that warrant more detailed investigation.

Note that in this library, the function is referenced by a **function index** rather than as a string. Make note of the function indices of functions of interest.

More detailed profiling information on individual functions can be obtained using,
```
pdb.getFunctionProfile(function_index, 'exclusive')
pdb.getFunctionProfile(function_index, 'inclusive')
```

Assuming the HBOS (default) or COPOD algorithm was used to identify anomalies, it is also possible to obtain a complete histogram of exclusive execution times using

```
edges, counts = pdb.getFunctionADmodelHistogram(function_index)
```

which can be plotted using, e.g.,

```
import matplotlib as mpl
mpl.pyplot.hist(edges[:-1], edges, weights=counts);
mpl.pyplot.show()
```


### Identifying analysis targets

Functions that may deserve more detailed analysis can also be identified using 
```pdb.topfunctions(sort_by)```
where `sort_by` can be 
1. `anom_severity` - the accumulated anomaly severity - a metric of anomaly importance.
2. `anom_count` - the count of anomalies.
3. `total_time_excl` - the total exclusive runtime of the function.

### Call stacks

Anomalies and normal executions are tagged by Chimbuko based on a model indexed by the function name, but in practice there are many call paths/stacks that can execute a specific function. Summaries of which call stacks were associated with the collection of stored anomalies and/or normal executions can be obtained using
```
pdb.getFunctionCallStackLabelsAndCounts(function_index, subset)
```
where `subset` can be `anomalies`, `normal_execs`, `both`. The call stacks are assigned a hash index, which can be translated into a call stack using
```
pdb.getLabeledCallStack(call_stack_hash)
```

### Function anomalies and normal executions

The anomalous and normal executions (henceforth, *events*) for a specific function can be obtained using
```
pdb.getFunctionEvents(function_index, "anomalies")
pdb.getFunctionEvents(function_index, "normal_execs")
```

Individual events are referred to by a unique event index (**event_id**), e.g. `0:0:231:435154`. Take note of events of interest.

The time distribution of anomalies on a specific program index (**pid**) and rank (**rid**) can be obtained using
```
times = pdb.getAnomalyTimes(pid,rid)
func_times = pdb.getAnomalyTimes(pid,rid,function_index)
```
where the second version specified a specific function of interest. These can be plotted as, e.g.
```
import matplotlib as mpl
mpl.pyplot.hist(times, bins=100);
mpl.pyplot.xlabel("application run time (s)")
mpl.pyplot.xlabel("anomaly count")
mpl.pyplot.show()
```

### Detailed event information

For a given event, a table detailing function executions occuring in a time window around the event on the same thread, can be obtained using
```
pdb.getEventExecWindow(event_id)
```

One can also obtain the node memory and CPU usage information using
```
pdb.getEventNodeMemoryStatus(event_id)
pdb.getEventNodeCPUstatus(event_id)
```
Note, however, that this information is collected only periodically, and so the sampling time may not align with the function execution.
