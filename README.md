
# Simple example for access count in Apache logs using Apache Flink

Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications

https://flink.apache.org/


### Apache Logs 

```access.log```

```
...
lordgun.org              - - [07/Mar/2004:17:01:53 -0800] "GET /razor.html                                                                               HTTP/1.1" 200 2869
64.242.88.10             - - [07/Mar/2004:17:09:01 -0800] "GET /twiki/bin/search/Main/SearchResult?scope=text&regex=on&search=Joris%20*Benschop[^A-Za-z] HTTP/1.1" 200 4284
64.242.88.10             - - [07/Mar/2004:17:10:20 -0800] "GET /twiki/bin/oops/TWiki/TextFormattingRules?template=oopsmore&param1=1.37&param2=1.37       HTTP/1.1" 200 11400
64.242.88.10             - - [07/Mar/2004:17:13:50 -0800] "GET /twiki/bin/edit/TWiki/DefaultPlugin?t=1078688936                                          HTTP/1.1" 401 12846
64.242.88.10             - - [07/Mar/2004:17:16:00 -0800] "GET /twiki/bin/search/Main/?scope=topic&regex=on&search=^g                                    HTTP/1.1" 200 3675
64.242.88.10             - - [07/Mar/2004:17:17:27 -0800] "GET /twiki/bin/search/TWiki/?scope=topic&regex=on&search=^d                                   HTTP/1.1" 200 5773
lj1036.inktomisearch.com - - [07/Mar/2004:17:18:36 -0800] "GET /robots.txt                                                                               HTTP/1.0" 200 68
lj1090.inktomisearch.com - - [07/Mar/2004:17:18:41 -0800] "GET /twiki/bin/view/Main/LondonOffice                                                         HTTP/1.0" 200 3860
...
```


### Usage

```
$ ./bin/pyflink.sh web_log_analysis.py - access.log 

```


```access.log.count```

```
IP           , URL                                                , COUNT
...
10.0.0.153   , /cgi-bin/mailgraph.cgi/mailgraph_3_err.png         , 12
10.0.0.153   , /mailman/options/ppwc/ppwctwentynine@shaw.com      , 1
10.0.0.153   , /twiki/pub/TWiki/TWikiLogos/twikiRobot46x50.gif    , 4
10.0.0.153   , /mailman/options/ppwc/ppwctwentynine--at--shaw.com , 1
64.242.88.10 , /ie.htm                                            , 1
64.242.88.10 , /RBL.html                                          , 1
64.242.88.10 , /rfc.html                                          , 1
64.242.88.10 , /razor.html                                        , 1
64.242.88.10 , /robots.txt                                        , 2
...
```

### Program in Flink

As we already saw in the example, Flink programs look like regular python programs. Each program consists of the same basic parts:

* Obtain an Environment,
* Load/create the initial data,
* Specify transformations on this data,
* Specify where to put the results of your computations, and
* Execute your program.

#### Code

Complete program in ```web_log_analysis.py```

```python
...

# FILTER log lines, returns True (Accept) only
# for rows that have the IP in the linst
class IPFilter(FilterFunction):
    def filter(self, value):
        data = value.split(" ")
        ip   = data[0]

        ip_filtered = [
            'ogw.netinfo.bg',
            '64.242.88.10',
            '10.0.0.153'
        ]
        
        return ip in ip_filtered

# MAP each line with IP, PAGE accessed and COUNTER
#
class MapPageByIP(MapFunction):
    def map(self, value):
        data = value.split(" ")
        ip   = data[0]
        page = data[6]

        return (ip, page, 1)
            
# REDUCE when adding (COUNT) IP and PAGE tuple
#
class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        ip, page, count = iterator.next()

        # Total access in the page/ip
        count += sum([x[2] for x in iterator])

        collector.collect((ip, page, count))


if __name__ == "__main__":
    # set up the environment with a source (in this case from a text file
    env = get_environment()
    
    # Read log
    data    = env.read_text(sys.argv[1])
    
    # Process
    result  = data \
                .filter(IPFilter()) \
                .map(MapPageByIP()) \
                .group_by(0, 1) \
                .reduce_group(Adder(), combinable=True) \
                .write_csv("{}.count".format(sys.argv[1]), \
                            write_mode=WriteMode.OVERWRITE)

    # build the job flow
    env.execute(local=True)
```