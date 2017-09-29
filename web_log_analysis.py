import os
import sys

from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.MapFunction import MapFunction
from flink.functions.FilterFunction import FilterFunction

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