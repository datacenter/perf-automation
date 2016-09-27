import os, requests
import json
import sys, datetime
import fnmatch
import os, time
from utility import *
import threading
from jsonrpclib import Server
import shutil

inputTag = sys.argv[1]+sys.argv[2]
inputData = {}
with open(os.getcwd() + os.path.sep + "switchElastic.json") as configFile:
    inputData = json.load(configFile)

swtDetails = get_switch_details(inputData['switch_config'])

for xSwt in swtDetails.keys():
    print '+'+'-'*60+'+'
    if swtDetails[xSwt]['type'].lower() != 'arista':
        exec_swt_cmd(swtDetails[xSwt], 'clear counter buffers')
        exec_swt_cmd(swtDetails[xSwt], 'clear qos statistics')
        exec_swt_cmd(swtDetails[xSwt], 'clear counter interface all')
    else:
        swtObj = Server('https://' +  swtDetails[xSwt]['user'] + ':' + swtDetails[xSwt]['passwd'] + '@' + swtDetails[xSwt]['ip'] + ':443/command-api')
        print 'Clearing Arista Switch Counters.'
        response = swtObj.runCmds( 1, [ { "cmd": "enable", "input": "cisco123" },  "clear counters" ] )
        print response
    

# Create output dir for Arista json files
out_dir = os.getcwd() + os.path.sep + 'output' + os.path.sep + inputTag
if os.path.exists(out_dir):
    shutil.rmtree(out_dir)
os.makedirs(out_dir)

# Get cdp neighbor Details
for xSwt in swtDetails.keys():
    if swtDetails[xSwt]['type'].lower() != 'arista':
        print '+'+'-'*60+'+'
        resp = exec_swt_cmd(swtDetails[xSwt], 'show cdp neighbors')
        data = json.loads(resp.text)['result']['body']['TABLE_cdp_neighbor_brief_info']['ROW_cdp_neighbor_brief_info']
        swtDetails[xSwt]['cdp_neighbors'] = data 
        update_ESC(inputData['es_url'] + 'monitorswitch/cdp_neighbors', swtDetails[xSwt]['name'], inputTag, [ 'data' ], [ data ])
    else:
        print '+'+'-'*60+'+'
        cmd = 'show lldp neighbors'
        print 'Executing command :', cmd 
        swtObj = Server('https://' +  swtDetails[xSwt]['user'] + ':' + swtDetails[xSwt]['passwd'] + '@' + swtDetails[xSwt]['ip'] + ':443/command-api')
        response = swtObj.runCmds( 1, [cmd] )
        swtDetails[xSwt]['cdp_neighbors'] = response[0]['lldpNeighbors']
        update_ESC(inputData['es_url'] + 'monitorswitch/cdp_neighbors', swtDetails[xSwt]['name'], inputTag, [ 'data' ], [ response[0]['lldpNeighbors'] ])

# Get Hardware Mapping
payload =  [{"id": 1,"jsonrpc": "2.0", "method": "cli", "params": {"cmd": "", "version": 1 } }]
for xSwt in swtDetails.keys():
    print '+'+'-'*60+'+'
    if swtDetails[xSwt]['type'].lower() != 'arista':
        myheaders={'content-type':'application/xml'}
        payload[0]['params']['cmd'] = 'show interface hardware-mappings'
        print 'Executing command :', payload[0]['params']['cmd']
        resp = requests.post(swtDetails[xSwt]['url'], data='<?xml version="1.0"?><ins_api><version>1.0</version><type>cli_show</type><chunk>0</chunk><sid>sid</sid><input>show interface hardware-mappings</input><output_format>xml</output_format></ins_api>',  \
                                        headers=myheaders, auth=(swtDetails[xSwt]['user'], swtDetails[xSwt]['passwd']))
        print 'SWT(%s) Response Code :%s'%(swtDetails[xSwt]['name'], resp)
        data = parse_xml_resp(resp)
        swtDetails[xSwt]['hardware_mapping'] = data 
        update_ESC( inputData['es_url'] + 'monitorswitch/hardware_mapping', swtDetails[xSwt]['name'], inputTag, [ 'data' ], [ data ])
    else:
        print 'Arista hardware mapping - TBD'

# Configure Server Monitoring Tag
print '+'+'-'*60+'+'
print 'Configure Server Monitoring Tag'
update_ESC_Tag(inputData['es_url'] + 'ceph/clusterTag', inputTag)
print '+'+'-'*60+'+'

# Configure job details 
print 'Updating Job Details to ESC'
with open('swtDetails.json', 'w') as f:
    f.write(json.dumps(swtDetails))

jobDetails = {}
jobDetails['Tag'] = inputTag
jobDetails['jobname'] = sys.argv[3]
jobDetails['jobid'] = 'Not Available'
jobDetails['starttime'] = time.time()
jobDetails['elapsed'] = 'Not Applicable'
jobDetails['status'] = '-'
jobDetails['tcpwindow'] = sys.argv[4] 
jobDetails['nodecount'] = sys.argv[5] 
jobDetails['desc'] = 'Not Given'
jobDetails['drop'] = 0
jobDetails['rx_pause'] = 0
jobDetails['tx_pause'] = 0
jobDetails['pk_buff_if'] = 0
jobDetails['segtran'] = 0
jobDetails['reset'] = 0
jobDetails['srv_drop'] = 0
jobDetails['pk_buf_swt'] = 0.0

hdr, data_row = [], []
for key, value in jobDetails.iteritems():
    if key == 'starttime':
        continue
    hdr.append(key)
    data_row.append(value)
update_ESC(inputData['es_url'] + 'monitorswitch/job_details', ','.join(swtDetails.keys()), inputTag, hdr, data_row)

with open(os.getcwd() + os.path.sep + 'jobDetails.json', 'w') as f:
    f.write(json.dumps(jobDetails))

# Collect netstat Details
print '+'+'-'*60+'+'
start_time = time.time()
print 'Collecting netstat/ip/iostat details for cluster nodes'
data = get_node_ip_netstat_details()
end_time = time.time()
print 'Netstat/ip Execution Time :',end_time - start_time
update_ESC(inputData['es_url'] + 'monitorswitch/netstat', None, inputTag, [ 'data' ], [ data ])

# Collect Server dynamic data
inputData['dynamic_monitor'] = 1
with open(os.getcwd() + os.path.sep + "switchElastic.json", 'w') as configFile:
    configFile.write(json.dumps(inputData))
print '+'+'-'*60+'+'
print 'Started Collecting dynamic details for cluster nodes'
os.system('python monitor.py ' + inputTag + ' &')
