#!/usr/bin/env python

# Get data from Zerto API, organize, and send to Graphite

__author__ = 'Ben Pingilley'

import requests
import json
import collections
import time
import datetime
import socket
import argparse
import yaml
import sys
import pprint

# Suppress InsecureRequestWarning output
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# define and gather command line options
parser = argparse.ArgumentParser(description='Collects data from Zerto server and stores in Graphite.')


parser.add_argument(
    "site",
    help="Specify the site of the Zerto Server"
)

parser.add_argument(
    "-g",
    "--graphite",
    dest="graphite",
    help="Specify the fqdn of the Graphite Server"
)

parser.add_argument(
    "-p",
    "--port",
    dest="port",
    type=int,
    default=2023,
    help="Specify the port for the Graphite Server"
)

parser.add_argument(
    "-x",
    "--prefix",
    dest="prefix",
    default="zerto",
    help="Specify the prefix for storing the Graphite data"
)

parser.add_argument(
    "-pp",
    "--pretty",
    dest="pretty",
    action='store_true',
    help="Print Graphite JSON data"
)

# Parse Arguments
options = parser.parse_args()

# Define Graphite Information
graphite_address = options.graphite
graphite_port = options.port
graphite_prefix = options.prefix

# Time Variables
# For Linux. Does not work on Windows.
# timestamp = str(datetime.datetime.now().strftime('%s'))[0:10]
timestamp = str(time.time())[0:10]
fromTime = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:00")
toTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")

# Create Dictionary to Store Targes (Hosts/Clusters)
targets = collections.defaultdict(dict)

# Load config file which matches site to zerto url
with open("servers.yaml", 'r') as yamlfile:
    servers = yaml.load(yamlfile)

# Check if Zerto server is configured for given site
if options.site in servers:
    fqdn = servers[options.site]
else:
    sys.exit("Zerto Site Does Not Exist")

# Zerto API Request
url = 'https://%s:9669/zvmservice/ResourcesReport/getSamples' % (fqdn)
params = dict(
    fromTimeString=fromTime,
    toTimeString=toTime,
    startIndex='0',
    count='100'
)
# Responsed with JSON
headers = dict(
    Accept='application/json'
)

# Make Request and Store in Variable
resp = requests.get(url=url, params=params, headers=headers, verify=False)

# Convert response text to JSON
json_object = json.loads(resp.text)

# Print returned JSON if user requested
if options.pretty == True:
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(json_object)

# Iterate Through JSON Object
for target in json_object:
    # Determine if Target is a Cluster
    if target['TargetCluster'] != '':
        host = target['TargetCluster']
    else:
        continue

    # Remove domain and replace spaces with dashes
    host = host.replace(' ','-')
    host = host.replace('.+','')

    # Add site prefix to host name
    host = "%s.%s" % (options.site, host)

    # Gather All Relevent Information and Store in Dictionary
    if host in targets:
        for key, value in target.iteritems():
            if isinstance(value, (int, long, float)):
                targets[host][key] += int(target[key])
    else:
        for key, value in target.iteritems():
            if isinstance(value, (int, long, float)):
                targets[host][key] = int(target[key])

# Iterate Through Target Dictionary
for target, metrics in targets.iteritems():
    message = []
    for key, value in metrics.iteritems():
        # Create Zerto Formatted Message with Relevent Information
        message.append(["{}.{}.{} {} {}".format(graphite_prefix, target, key, value, timestamp)])


    # Define Connection
    conn = socket.socket()
    # Open Connection
    conn.connect((graphite_address, graphite_port))
    for i in xrange(0, len(message)):
      msgOutput = message[i]
      msgOutput = str(message[i]).strip("['']")
      msgOutput = "{}\n".format(msgOutput)
      conn.sendall(msgOutput)
    # Close Connection
    conn.close()
    