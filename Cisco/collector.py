import os, requests
import json
import sys, datetime
import fnmatch
from utility import *
import time
import shutil
from jsonrpclib import Server

inputTag, jobName = '', ''

inputTag = sys.argv[1] + sys.argv[2]
jobName = sys.argv[3]

inputData = {}
with open(os.getcwd() + os.path.sep + "switchElastic.json") as configFile:
    inputData = json.load(configFile)

static_monitoring = False
if int(inputData['monitoring_interval']) != -1:
    static_monitoring = True

swtDetails = {}
with open('swtDetails.json', 'r') as f:
    swtDetails = json.load(f)

jobDetails = {}
with open(os.getcwd() + os.path.sep + 'jobDetails.json', 'r') as f:
    jobDetails = json.load(f)

firstTime, iter = True, 0
jobDetails['jobid'] = ''

if static_monitoring:
    print 'No Hadoop Job Started'
else:
    print 'Job :', jobName
    if jobName == 'dfsio':
        print 'Added additional '+ inputData['dfsio_wait'] +' second wait for dfsio job'
        time.sleep(int(inputData['dfsio_wait']))
    else:
        time.sleep(1)

monitor_time = 0
while(True):
    print '+'+'-'*60+'+'
    sleep_with_progressBar(int(inputData['polling_interval']))

    if not static_monitoring:
        if jobName == 'spark_terasort':
            print 'Cheking if spark terasort is running'
            out = os.popen("ps -ef | grep 'com.github.ehiggs.spark.terasort.TeraSort'")
            print out.read()
            out = os.popen("ps -ef | grep 'com.github.ehiggs.spark.terasort.TeraSort' | wc -l")
            count = int(out.read().strip())
            print 'process count --> ', count
            if count < 3:
                print 'breaking while'
                break
        else:
            ret = is_hadoop_job_running()
            if not ret:
                print 'Job Not Running'
                break 
            else:
                print 'Job is Running'

            # Get Interface Traffic
            if jobDetails['jobid'] == '':
                jobDetails['jobid'] = get_hadoop_job_id()

    monitor_time += int(inputData['polling_interval'])
    iter += 1
    print '\nIteration No :', iter

    print '\nCollecting Interface Traffic details.'
    for xSwt in swtDetails.keys():
        if swtDetails[xSwt]['type'].lower() != 'arista':
            intf_data = []
            for xNeighbor in swtDetails[xSwt]['cdp_neighbors']:
                intf_stats = {} 

                if xNeighbor['intf_id'] == 'mgmt0':
                    continue

                resp = exec_swt_cmd(swtDetails[xSwt], 'show interface ' + xNeighbor['intf_id'], 0)
                intf_stats[xNeighbor['intf_id']] = json.loads(resp.text)['result']['body']['TABLE_interface']['ROW_interface']
                intf_data.append(intf_stats)

                sys.stdout.write('\r')
                sys.stdout.write("%s" % ('.'*len(intf_data)))
                sys.stdout.flush()

            data1 = intf_data[:len(intf_data)/2]
            data2 = intf_data[len(intf_data)/2:]
            update_ESC(inputData['es_url'] + 'monitorswitch/interface_stats', swtDetails[xSwt]['name'], inputTag,['data1', 'data2' ], [ data1, data2 ])
        else:
            intf_data = []
            for xNeighbor in swtDetails[xSwt]['cdp_neighbors']:
                intf_stats = {} 
                
                cmd = 'show interfaces ' + xNeighbor['port']
                #print 'Executing command :', cmd
                swtObj = Server('https://' +  swtDetails[xSwt]['user'] + ':' + swtDetails[xSwt]['passwd'] + '@' + swtDetails[xSwt]['ip'] + ':443/command-api')
                response = swtObj.runCmds( 1, [cmd] )
                intf_stats[xNeighbor['port']] = response[0]['interfaces'][xNeighbor['port']]
                intf_data.append(intf_stats)

            data1 = intf_data[:len(intf_data)/2]
            data2 = intf_data[len(intf_data)/2:]
            update_ESC(inputData['es_url'] + 'monitorswitch/interface_stats', swtDetails[xSwt]['name'], inputTag,['data1', 'data2' ], [ data1, data2 ])

    print '+'+'-'*60+'+'
    print 'Collecting netstat details for cluster nodes'
    start_time = time.time()
    data = get_node_ip_netstat_details()
    end_time = time.time()
    print 'Netstat Execution Time :',end_time - start_time
    update_ESC( inputData['es_url'] + 'monitorswitch/netstat', None, inputTag, [ 'data' ], [ data ])

    if static_monitoring == True and monitor_time >= int(inputData['monitoring_interval']):
        print 'breaking for static monitoring timespan'
        break

os.system("ps -ef | grep monitor.py | awk {'print $2'} | xargs kill")
inputData['dynamic_monitor'] = 0
with open(os.getcwd() + os.path.sep + "switchElastic.json", 'w') as configFile:
    configFile.write(json.dumps(inputData))

print '+'+'-'*60+'+'
print 'Collecting netstat/ip/iostat details for cluster nodes'
data = get_node_ip_netstat_details()
update_ESC( inputData['es_url'] + 'monitorswitch/netstat', None, inputTag, [ 'data' ], [ data ])

# Reset Server Monitoring Tag
print '+'+'-'*60+'+'
print 'UnConfigure the tag for server Monitoring'
update_ESC_Tag(inputData['es_url'] + 'ceph/clusterTag', 'default')


# Arista -
out_dir = os.getcwd() + os.path.sep + 'output' + os.path.sep + inputTag

# Get Switch Buffer Details
print '+'+'-'*60+'+'
packetDetails = []
for xSwt in swtDetails.keys():
    if swtDetails[xSwt]['type'].lower() != 'arista':
        print 'Collecting Switch Buffer details.'
        print '+'+'-'*60+'+'
        resp = exec_swt_cmd(swtDetails[xSwt], 'show hardware internal buffer info pkt-stats')
        data = json.loads(resp.text)['result']['body']['TABLE_module']['ROW_module']['TABLE_instance']['ROW_instance']
        update_ESC(inputData['es_url'] + 'monitorswitch/buffer_stats', swtDetails[xSwt]['name'], inputTag,[ 'data' ], [ data ])
    else:
        print '+'+'-'*60+'+'
        print xSwt + ' Collecting Queue Monitor Data'
        swt_dir = out_dir + os.path.sep + xSwt
        if not os.path.exists(swt_dir):
            os.makedirs(swt_dir)

        swtObj = Server('https://' +  swtDetails[xSwt]['user'] + ':' + swtDetails[xSwt]['passwd'] + '@' + swtDetails[xSwt]['ip'] + ':443/command-api')
        response = swtObj.runCmds( 1, ["show queue-monitor length limit "+ str(600) + " seconds"] )
        with open(swt_dir + os.path.sep + 'queue_data.json', 'w') as f:
            f.write(json.dumps(response))
        #update_ESC( es_url + 'aristaswitch/queue_stats', swt_details['name'], inputTag, [ 'data' ], response )

# Get Hardware Buffer Details
print '+'+'-'*60+'+'
packetDetails = []
for xSwt in swtDetails.keys():
    if swtDetails[xSwt]['type'].lower() != 'arista':
        print 'Collecting Hardware Buffer details.'
        print '+'+'-'*60+'+'
        resp = exec_swt_cmd(swtDetails[xSwt], 'show hardware internal buffer info pkt-stats peak')
        print 'buffer_peak_stat :', resp
        data = json.loads(resp.text)['result']['body']['TABLE_module']['ROW_module']['TABLE_instance']['ROW_instance']
        update_ESC(inputData['es_url'] + 'monitorswitch/buffer_peak_stats', swtDetails[xSwt]['name'], inputTag,[ 'data' ], [ data ])
    else:
        print '+'+'-'*60+'+'
        swtObj = Server('https://' +  swtDetails[xSwt]['user'] + ':' + swtDetails[xSwt]['passwd'] + '@' + swtDetails[xSwt]['ip'] + ':443/command-api')
        print xSwt +' Collecting Platform arad egress'
        response = swtObj.runCmds( 1, ["enable", "show platform arad egress"] )
        with open(swt_dir + os.path.sep + 'arad_egress.json', 'w') as f:
            f.write(json.dumps(response))

        print xSwt +' Collecting Platform Counters'
        response = swtObj.runCmds( 1, ["enable", "show platform arad counters"], "text" )
        with open(swt_dir + os.path.sep + 'arad_counters.txt', 'w') as f:
            f.write(str(response))


if static_monitoring:
    print "Monitoring Complete"
    time.sleep(5)
    sys.exit(0)

print '+'+'-'*60+'+'
# Check to verify if job started
if jobDetails['jobid'] == '' and not static_monitoring:
    print 'Job did not started successfully !!'
    sys.exit(1)

# Updating job details to Elastic Search
lines = []
with open('log', 'r') as f:
    lines = f.readlines()

start, end = '', ''
for line in lines:
   if 'Running job:' in line: 
      start = line.split()[1]
   if 'completed successfully' in line:
      end = line.split()[1]

print 'start :', start
print 'end:', end 
elapse = '0:0'
if start and end:
    delta = datetime.datetime.strptime(end, '%H:%M:%S') - datetime.datetime.strptime(start, '%H:%M:%S')
    elapsed = str(delta.seconds/60) + ':' + str(delta.seconds%60)
else:
    pass
print "elapsed :", elapsed

jobDetails['elapsed'] = elapsed
jobDetails['status'] = 'Complete'
hdr, data_row = [], []
for key, value in jobDetails.iteritems():
    if key == 'starttime':
        continue
    hdr.append(key)
    data_row.append(value)
update_ESC(inputData['es_url'] + 'monitorswitch/job_details', ','.join(swtDetails.keys()), inputTag, hdr, data_row)
#print zip(hdr, data_row)
with open(os.getcwd() + os.path.sep + 'jobDetails.json', 'w') as f:
    f.write(json.dumps(jobDetails))

time.sleep(5)
