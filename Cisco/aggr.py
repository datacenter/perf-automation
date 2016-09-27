import os, requests
import json
import sys
from utility import *

inputData = {}
inputTag = sys.argv[1] + sys.argv[2]
with open(os.getcwd() + os.path.sep + "switchElastic.json") as configFile:
    inputData = json.load(configFile)

swtDetails = {}
with open('swtDetails.json', 'r') as f:
    swtDetails = json.load(f)

print '+'+'-'*60+'+'
print 'Reading CDP Neighbor Data'
neighbor_data = {}
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/cdp_neighbors', inputTag)
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag: 
        neighbor_data[row['switch']] = json.loads(row['data'])
#print json.dumps(neighbor_data)

print '+'+'-'*60+'+'
print 'Reading HW Mapping Data'
hw_mapping_data = {}
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/hardware_mapping', inputTag)
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        hw_mapping_data[row['switch']] = json.loads(row['data'])
#print json.dumps(hw_mapping_data)


print '+'+'-'*60+'+'
print 'Reading Network Interface Statistics'
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/interface_stats', inputTag)
recCount = 0
intf_data = []
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        recCount += 1
        intf_data.extend(json.loads(row['data1']))
        intf_data.extend(json.loads(row['data2']))
#print json.dumps(intf_data)

print '+'+'-'*60+'+'
print 'Reading Buffer Statistics'
buff_data = {} 
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/buffer_stats', inputTag)
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        buff_data[row['switch']] = json.loads(row['data'])
#print json.dumps(buff_data)

print '+'+'-'*60+'+'
print 'Reading Buffer Peak Statistics'
peak_buff_data = {} 
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/buffer_peak_stats', inputTag)
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        peak_buff_data[row['switch']] = json.loads(row['data'])

#with open('test.json', 'w') as f:
#print json.dumps(peak_buff_data)

print '+'+'-'*60+'+'
print 'Reading Netstat Details'
netstat_data = {} 
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/netstat', inputTag)
ns_records = []
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        ns_records.append(json.loads(row['data']))

print '+'+'-'*60+'+'
print 'Reading Dynamic Node Data Details'
node_dyn_data = {} 
data = read_data_ESC(inputData['es_url']+ 'monitorswitch/node_stat', inputTag)
nd_records = []
for xitem in data['hits']['hits']:
    row = xitem['_source'] 
    if row['Tag'] == inputTag:
        nd_records.append(json.loads(row['stat']))

#with open('test.json', 'w') as f:
#print json.dumps(peak_buff_data)
print '+'+'-'*60+'+'
print 'Record count for each interface -',recCount
print 'Writing Interface View ( aggregated )'
intf_hdr = [ 'device', 'intf', 'inst', 'Smod', 'Sport', 'Avg_Rx(mbps)', 'Peak_Rx(mbps)', 'Avg_Tx(mbps)', 'Peak_Tx(mbps)', 'drop', 'Rx_pause', 'Tx_pause', 'buffer', 'segtran', 'reset', 'srv_drop',  'iostat_avg_read(MB)', 'iostat_avg_wrtn(MB)' ]
all_drop_sum, all_rx_pause_sum, all_tx_pause_sum, all_buffer_max = 0, 0, 0, 0 
sum_resets, sum_segtrans, sum_srv_drop = 0, 0, 0
print '-' * 50
for xSwt, neighbor_data in neighbor_data.iteritems():

    if 'arista' in xSwt.lower():
        # GAMGAM : START 
        for a_item in neighbor_data:
            if a_item['port'] == 'mgmt0':
                continue

            data_row = []
            segtrans, resets, srv_drop = 0, 0, 0
            a_data = []
            a_data.extend([ a_item['neighborDevice'], a_item['port'] ])
            b_data, c_data = [], []

            '''
            for b_item in hw_mapping_data[xSwt]:
                if a_item['intf_id'].replace('Ethernet', 'Eth') == b_item['Name']:
                    b_data = [ b_item['Smod'], b_item['SPort'], b_item['Slice'] ] 
            '''
            b_data = [ '', '', '' ] 

            rx_drop, tx_drop = 0, 0
            if a_item['neighborDevice'] in ns_records[0].keys():
                segtrans = abs(ns_records[0][a_item['neighborDevice']]['segments'] - ns_records[1][a_item['neighborDevice']]['segments'])
                resets = abs(ns_records[0][a_item['neighborDevice']]['resets'] - ns_records[1][a_item['neighborDevice']]['resets'])
                rx_drop = abs(int(ns_records[0][a_item['neighborDevice']]['rx_drop']) - int(ns_records[1][a_item['neighborDevice']]['rx_drop']))  
                tx_drop = abs(int(ns_records[0][a_item['neighborDevice']]['tx_drop']) - int(ns_records[1][a_item['neighborDevice']]['tx_drop']))  

            sum_resets += resets 
            sum_segtrans += segtrans
            sum_srv_drop += ( rx_drop + tx_drop ) 

            iter = 0
            sum_rx, sum_tx, peak_rx, peak_tx, max_rx_pause, max_tx_pause, max_drp_pkts = 0, 0, 0, 0, 0, 0, 0
            avg_rx, avg_tx = 0, 0
            sum_io_read, sum_io_write, avg_io_read, avg_io_write = 0, 0, 0, 0
            for x_intf in intf_data:
                for intfId, c_item in x_intf.iteritems():
                    if a_item['port'] == intfId:
                        iter += 1
                        sum_rx +=  int(c_item['interfaceStatistics']['inBitsRate'])
                        sum_tx +=  int(c_item['interfaceStatistics']['outBitsRate'])

                        if c_item.has_key('sum_kb_read'):
                            sum_io_read += c_item['sum_kb_read']

                        if c_item.has_key('sum_kb_wrtn'):
                            sum_io_write += c_item['sum_kb_wrtn']

                        '''
                        if int(c_item['eth_ignored']) > max_drp_pkts:
                            max_drp_pkts = int(c_item['eth_ignored'])
                        '''
                        if int(c_item['interfaceCounters']['inputErrorsDetail']['rxPause']) > max_rx_pause:
                            max_rx_pause = int(c_item['interfaceCounters']['inputErrorsDetail']['rxPause'])

                        if int(c_item['interfaceCounters']['outputErrorsDetail']['txPause']) > max_tx_pause:
                            max_tx_pause = int(c_item['interfaceCounters']['outputErrorsDetail']['txPause'])

                        if int(c_item['interfaceStatistics']['inBitsRate']) > peak_rx:
                            peak_rx = int(c_item['interfaceStatistics']['inBitsRate'])

                        if int(c_item['interfaceStatistics']['outBitsRate']) > peak_tx:
                            peak_tx = int(c_item['interfaceStatistics']['outBitsRate'])

                        print 'sum_rx, sum_tx, sum_io_read, sum_io_write, max_drp_pkts, max_rx_pause, max_tx_pause :', sum_rx, sum_tx, sum_io_read, sum_io_write, max_drp_pkts, max_rx_pause, max_tx_pause

            if iter != 0:
                avg_rx = (sum_rx * 1.0)/iter 
                avg_tx = (sum_tx * 1.0)/iter 
                avg_io_read = (sum_io_read * 1.0)/iter
                avg_io_write = (sum_io_write * 1.0)/iter
     
            all_rx_pause_sum += max_rx_pause 
            all_tx_pause_sum += max_tx_pause
            all_drop_sum += max_drp_pkts

            '''
            buff_val = 0
            for x_item in peak_buff_data[xSwt]:
                if x_item['instance'] == b_data[2]:
                    for y_item in x_item['TABLE_peak']['ROW_peak'][1:]:
                        if y_item['oport'] == b_data[1]:
                            buff_val = int(y_item['count_0'])

            if buff_val > all_buffer_max:
                all_buffer_max = buff_val
            '''
            buff_val = 0
            all_buffer_val = 0
            
            data_row.extend(a_data)
            data_row.extend(b_data)
            data_row.extend([ avg_rx/pow(2,20), peak_rx/pow(2,20), avg_tx/pow(2,20), peak_tx/pow(2,20), max_drp_pkts, max_rx_pause, max_tx_pause, buff_val, segtrans, resets, srv_drop, avg_io_read, avg_io_write])
            update_ESC(inputData['es_url'] + 'monitorswitch/intf_view', xSwt, inputTag, intf_hdr, data_row, 0)
            # GAMGAM : END

    else:
        for a_item in neighbor_data:
            #print 'neighbor -  a_item :', a_item

            if a_item['intf_id'] == 'mgmt0':
                continue

            data_row = []
            segtrans, resets, srv_drop = 0, 0, 0
            a_data = []
            a_data.extend([ a_item['device_id'], a_item['intf_id'] ])
            b_data, c_data = [], []

            for b_item in hw_mapping_data[xSwt]:
                if a_item['intf_id'].replace('Ethernet', 'Eth') == b_item['Name']:
                     b_data = [ b_item['Smod'], b_item['SPort'], b_item['Slice'] ] 

            rx_drop, tx_drop = 0, 0
            if a_item['device_id'] in ns_records[0].keys():
                segtrans = abs(ns_records[0][a_item['device_id']]['segments'] - ns_records[1][a_item['device_id']]['segments'])
                resets = abs(ns_records[0][a_item['device_id']]['resets'] - ns_records[1][a_item['device_id']]['resets'])
                rx_drop = abs(int(ns_records[0][a_item['device_id']]['rx_drop']) - int(ns_records[1][a_item['device_id']]['rx_drop']))  
                tx_drop = abs(int(ns_records[0][a_item['device_id']]['tx_drop']) - int(ns_records[1][a_item['device_id']]['tx_drop']))  

            sum_resets += resets 
            sum_segtrans += segtrans
            sum_srv_drop += ( rx_drop + tx_drop ) 

            iter = 0
            sum_rx, sum_tx, peak_rx, peak_tx, max_rx_pause, max_tx_pause, max_drp_pkts = 0, 0, 0, 0, 0, 0, 0
            avg_rx, avg_tx = 0, 0
            sum_io_read, sum_io_write, avg_io_read, avg_io_write = 0, 0, 0, 0
            for x_intf in intf_data:
                for intfId, c_item in x_intf.iteritems(): 
                    if a_item['intf_id'] == intfId:
                        iter += 1
                        sum_rx +=  int(c_item['eth_inrate1_bits'])
                        sum_tx +=  int(c_item['eth_outrate1_bits'])

                        if c_item.has_key('sum_kb_read'):
                            sum_io_read += c_item['sum_kb_read']

                        if c_item.has_key('sum_kb_wrtn'):
                            sum_io_write += c_item['sum_kb_wrtn']

                        if int(c_item['eth_ignored']) > max_drp_pkts:
                            max_drp_pkts = int(c_item['eth_ignored'])

                        if int(c_item['eth_inpause']) > max_rx_pause:
                            max_rx_pause = int(c_item['eth_inpause'])

                        if int(c_item['eth_outpause']) > max_tx_pause:
                            max_tx_pause = int(c_item['eth_outpause'])

                        if int(c_item['eth_inrate1_bits']) > peak_rx:
                            peak_rx = int(c_item['eth_inrate1_bits'])

                        if int( c_item['eth_outrate1_bits']) > peak_tx:
                            peak_tx = int(c_item['eth_outrate1_bits'])

                        print 'sum_rx, sum_tx, sum_io_read, sum_io_write, max_drp_pkts, max_rx_pause, max_tx_pause :', sum_rx, sum_tx, sum_io_read, sum_io_write, max_drp_pkts, max_rx_pause, max_tx_pause

            if iter != 0:
                avg_rx = (sum_rx * 1.0)/iter 
                avg_tx = (sum_tx * 1.0)/iter 
                avg_io_read = (sum_io_read * 1.0)/iter
                avg_io_write = (sum_io_write * 1.0)/iter
     
            all_rx_pause_sum += max_rx_pause 
            all_tx_pause_sum += max_tx_pause
            all_drop_sum += max_drp_pkts

            buff_val = 0
            for x_item in peak_buff_data[xSwt]:
                if x_item['instance'] == b_data[2]:
                    for y_item in x_item['TABLE_peak']['ROW_peak'][1:]:
                        if y_item['oport'] == b_data[1]:
                            buff_val = int(y_item['count_0'])

            if buff_val > all_buffer_max:
                all_buffer_max = buff_val

            data_row.extend(a_data)
            data_row.extend(b_data)
            data_row.extend([ avg_rx/pow(2,20), peak_rx/pow(2,20), avg_tx/pow(2,20), peak_tx/pow(2,20), max_drp_pkts, max_rx_pause, max_tx_pause, buff_val, segtrans, resets, srv_drop, avg_io_read, avg_io_write])
            update_ESC(inputData['es_url'] + 'monitorswitch/intf_view', xSwt, inputTag, intf_hdr, data_row, 0)

print '+'+'-'*60+'+'
print 'Writing Switch View ( aggregated )'
data_hdr = [ 'inst_0_peak', 'inst_0_util', 'inst_0_peak_util(%)',
             'inst_1_peak', 'inst_1_util', 'inst_1_peak_util(%)',
             'inst_2_peak', 'inst_2_util', 'inst_2_peak_util(%)',
             'inst_3_peak', 'inst_3_util', 'inst_3_peak_util(%)',
             'inst_4_peak', 'inst_4_util', 'inst_4_peak_util(%)',
             'inst_5_peak', 'inst_5_util', 'inst_5_peak_util(%)' ]
data_row, peak_usage = [], 0
inst_data = []
peak_buf_swt = 0.0
for xSwt, buffData in buff_data.iteritems():
    data_row = []
    for item in buffData: 
        peak = int(item['max_cell_usage_drop_pg'])
        total = int(item['total_instant_usage_drop_pg']) + int(item['rem_instant_usage_drop_pg'] )
        peak_util = 0
        if total != 0:
           peak_util = (peak * 100.0)/ total
        else:
           peak_util = 0

        if peak_util > peak_buf_swt:
            peak_buf_swt = peak_util
        #print 'peak, total, %',peak, total, peak_util
        data_row.extend([ peak, total, peak_util ])

    update_ESC(inputData['es_url'] + 'monitorswitch/switch_view', xSwt, inputTag, data_hdr, data_row)

print '+'+'-'*60+'+'
print 'Writing Node Data View ( aggregated )'
'''
data_hdr = [ 'CPU_Util(%)', 'Disk_Rd(MB/s)', 'Disk_Wr(MB/s)', 'Mem_Util(%)', 
             'NIC_Rx(Mb/s)', 'NIC_Tx(Mb/s)', 'hostId', 'hostname' ]
sum_cpu_util, sum_disk_rd, sum_disk_wr, sum_mem_util = 0, 0, 0, 0
sum_nic_rx, sum_nic_tx, host_id, host_name = 0, 0, 0, ''
'''

data_hdr = [ 'iter', 'CPU_Util(%)', 'Mem_Util(%)', 'Disk_Rd(MB/s)', 'Disk_Wr(MB/s)', 'hostname', 'hostid' ]
node_aggr = {}
rec_count = 0
for rec in nd_records:
    rec_count += 1
    for compute, compute_stat in rec.iteritems():
        data_row = []
        cpu_util = float(compute_stat['CPU_Util(%)']) * 1.0
        mem_util = float(compute_stat['Mem_Util(%)']) * 1.0

        disk_rd, disk_wr = 0, 0
        for each_disk in compute_stat['disk_stats']:
            disk_rd += each_disk['Disk_Rd(MB)']  
            disk_wr += each_disk['Disk_Wr(MB)']

        data_row.extend([ rec_count, cpu_util, mem_util, disk_rd, disk_wr, compute, int(compute.split('e')[1]) ])
        update_ESC(inputData['es_url'] + 'monitorswitch/node_stat_view', None, inputTag, data_hdr, data_row)

        '''
        print 'Data -', node_stat['CPU_Util(%)'], node_stat['Mem_Util(%)'] 
        if not node_aggr.has_key(node):
            node_aggr[node] = {}
            node_aggr[node]['CPU_Util(%)'] = node_stat['CPU_Util(%)']
            node_aggr[node]['Mem_Util(%)'] = node_stat['Mem_Util(%)'] 
        else:
            node_aggr[node]['CPU_Util(%)'] += node_stat['CPU_Util(%)']
            node_aggr[node]['Mem_Util(%)'] += node_stat['Mem_Util(%)'] 
        '''

print '+'+'-'*60+'+'+'\n'
# Updating job details to Elastic Search
jobDetails = {}
with open(os.getcwd() + os.path.sep + 'jobDetails.json', 'r') as f:
    jobDetails = json.load(f)

jobDetails['drop'] = all_drop_sum
jobDetails['rx_pause'] = all_rx_pause_sum
jobDetails['tx_pause'] = all_tx_pause_sum 
jobDetails['pk_buff_if'] = all_buffer_max
jobDetails['segtran'] = sum_segtrans
jobDetails['reset'] = sum_resets 
jobDetails['pk_buf_swt'] = peak_buf_swt
jobDetails['srv_drop'] = sum_srv_drop
hdr, data_row = [], []
for key, value in jobDetails.iteritems():
    if key == 'starttime':
        continue
    hdr.append(key)
    data_row.append(value)
update_ESC(inputData['es_url'] + 'monitorswitch/job_details', ','.join(swtDetails.keys()), inputTag, hdr, data_row)
#print zip(hdr, data_row)
