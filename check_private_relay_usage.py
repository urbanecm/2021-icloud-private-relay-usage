#!/usr/bin/env python

import ipaddress

from collections import defaultdict

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T, Window

# load iCloud's private relay egress ranges
# data comes from https://mask-api.icloud.com/egress-ip-ranges.csv
relay_ranges = pd.read_csv('/home/urbanecm/Documents/steward/icloud-private-relay-usage/egress-ip-ranges.csv', sep=',', names=['range', 'country', 'region', 'city', 'empty']).drop(columns=['empty'])

## This data structure is based on using https://stackoverflow.com/a/1004527
## to determine if an IP falls within a given network, which allows us to make
## the following assertions:
## 1: IPv4 and IPV6 networks are disjoint so we can split on IP version. There used to be
##    compatibility between the networks, but that was deprecated according to
##    https://networkengineering.stackexchange.com/questions/57903/are-the-ipv6-address-space-and-ipv4-address-space-completely-disjoint
## 2: The netmask is binary-AND'ed onto the binary IP address, hence
##    the second layer are the netmasks, of which we expect there to be a limited number.
## 3: We then have a limited set of possible networks which are all numbers
##    so we store those as a set and let Python handle it, which gives us fast lookup.
relay_nets = {
	'4' : defaultdict(set),
	'6' : defaultdict(set)
}

for net_raw in relay_ranges.range:
	net = ipaddress.ip_network(net_raw)

	net_v = str(net.version)

	relay_nets[net_v][net.netmask].add(int(net.network_address))

def is_ip_private_relay(ip_raw):
	try:
		ip = ipaddress.ip_address(ip_raw)
		bin_ip = int(ip)

		for netmask, range_set in relay_nets[str(ip.version)].items():
			bin_netmask = int(netmask)
			if (bin_ip & bin_netmask) in range_set:
				return(True)
	except ValueError: # not a valid IP address
		pass

	return(False)

# start spark session
spark = (
	SparkSession.builder.master("yarn")
	.appName("urbanecm-testing")
	.enableHiveSupport()
	.getOrCreate()
)

viewers_data = (
	spark.read.table("wmf.pageview_actor")
	# limit ourself to 2021-08-14, 10:00-12:00 UTC
	.where(F.col("year") == 2021)
	.where(F.col("month") == 8)
	.where((F.col("day") == 14) | (F.col("day") == 13) | (F.col("day") == 12))
	#.where((F.col("hour") == 12) | (F.col("hour") == 11) | (F.col("hour") == 10) | (F.col("hour") == 9) | (F.col("hour") == 8))

	# only cswiki, to limit processed data
	.where(F.col("normalized_host.project_family") == 'wikipedia')
	.where(F.col("normalized_host.project") == 'en')

	# only user pageview traffic
	.where(F.col("agent_type") == 'user')
	.where(F.col("is_pageview") == True)

	# exclude mobile app -- private relay does not affect it
	.where(F.col("access_method") != 'mobile app')

	# only iOS 15 devices
	.where(F.col("user_agent_map.os_family") == 'iOS')
	.where(F.col("user_agent_map.os_major") == '15')
)

viewers_data_is_relay = spark.createDataFrame(
	viewers_data.rdd.map(lambda r: T.Row(
		year=r.year,
		month=r.month,
		day=r.day,
		hour=r.hour,
		project="%s.%s" % (r.normalized_host.project, r.normalized_host.project_family),
		is_relay=is_ip_private_relay(r.ip)
	))
)

agg_viewer_data_by_relay = (
	viewers_data_is_relay
	.groupBy("year", "month", "day", "hour", "project", "is_relay")
	.agg(F.count("*").alias("views"))
)

# convert to pandas dataframe, add percentage and write to csv
pandas_df = agg_viewer_data_by_relay.toPandas()
cols = list(pandas_df.columns)
cols.remove('views')
cols.remove('is_relay')
pandas_df['%'] = np.round(pandas_df.views / pandas_df.groupby(cols).views.transform('sum') * 100, 2)
pandas_df.to_csv('/home/urbanecm/tmp/private_relay_usage_test.tsv', sep='\t', index=False)
