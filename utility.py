import os, sys
import requests
import json
import time
import paramiko


payload =  [{"id": 1,"jsonrpc": "2.0", "method": "cli", "params": {"cmd": "", "version": 1 } }]

def get_cur_time():
    command = 'date -u +\"%Y-%m-%d %H:%M:%S\"'
    f = os.popen(command)
    now = f.read()
    return now.strip()

def get_switch_details(configParams):
    swtDetails = {}
    swtList, typeList  = configParams['mgmtIp'].split(','), configParams['type'].split(',')
    usrList, passwdList = configParams['username'].split(','), configParams['password'].split(',')
    x = 0
    for xSwt, xUser, xPasswd, xType in zip(swtList, usrList, passwdList, typeList):
        x += 1
        swtDetails[xSwt] = {}
        swtDetails[xSwt]['name'] = 'Lacrosse' + str(x)
        swtDetails[xSwt]['url'] = 'http://' + xSwt + '/ins'
        swtDetails[xSwt]['user'], swtDetails[xSwt]['passwd'] = xUser, xPasswd
        swtDetails[xSwt]['type'] = xType
        # Read switch Hostnames as well
    return swtDetails

def exec_swt_cmd( swt, cmd, showResp=1 ):
    myheaders={'content-type':'application/json-rpc'}
    payload[0]['params']['cmd'] = cmd
    if showResp:
        print 'Executing command :', cmd
    response = requests.post(swt['url'],data=json.dumps(payload[0]), headers=myheaders,auth=(swt['user'], swt['passwd']))
    if showResp:
        print 'SWT(%s) Response Code :%s'%(swt['name'], response)
    return response

def update_ESC( url, swtName, tag, header, data, showResp=1):
    #print url, swtName, tag, showResp
    esData = {}
    esData['switch'], esData['Tag'], esData['timestamp'] = swtName, tag, get_cur_time()
    for th, td in zip(header, data):
        if th in ['data', 'data1', 'data2']:
            esData[th] = json.dumps(td)
        else:
            esData[th] = td

    if swtName is None:
        del esData['switch']

    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    payload = esData

    if esData.has_key('elapsed'):
        if ':' in esData['elapsed']:
            print '-- del -->', url + '/_query?q=Tag:' + tag 
            resp = requests.delete(url + '/_query?q=Tag:' + tag)
            print '-- del -->', resp, resp.text

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    if showResp:
        print 'ESC(%s) Response Code :%s'%(swtName, response)
    #print 'ESC Response Text :', response.text

def update_ESC_Tag( url, tag, showResp=1):
    esData = {}
    esData['Tag'], esData['Timestamp_Monitor'], esData['Cluster_name'] = tag, get_cur_time(), 'HADOOP1'
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    payload = esData
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    if showResp:
        print 'ESC Response Code :%s'%( response)
    #print 'ESC Response Text :', response.text

def read_data_ESC( url, tag):
    url = url + '/_search/?q=Tag:' + tag 
    res = requests.get(url)
    #print res
    data = json.loads(res.text)
    return data

def parse_xml_resp(content):
    line_count, dash_line_count = 0, 0
    allData = []

    for item in content.text.split('\n'):
        line_count += 1
        if item == '' or line_count < 20:
            continue
        elif line_count == 20:
            header = [ x for x in item.split(' ') if x!='' ]
            continue
        elif '----' in item:
            dash_line_count += 1
            continue
        elif dash_line_count > 1:
            break
        else:
            data = [ x for x in item.split(' ') if x!='' ]
        allData.append(dict(zip(header, data)))
    return allData

def sleep_with_progressBar(duration=30):
    print 'sleeping for '  + str(duration) + ' seconds'
    for i in range(0,duration+1):
        sys.stdout.write('\r')
        sys.stdout.write("[.%s%s.] %d/%d ... " % ('+'*i,' '*(duration-i), i, duration))
        sys.stdout.flush()
        time.sleep(1)

def is_hadoop_job_running():
    proc = os.popen('mapred job -list')
    for line in proc.read().split('\n'):
        if 'job_' in line:
            return True
    return False

def get_hadoop_job_id():
    proc = os.popen('mapred job -list')
    for line in proc.read().split('\n'):
        if 'job_' in line:
            return line.split()[0] 
    return False

def get_netstat_details():
    nodelist = []

    with open(os.getcwd() + os.path.sep + 'hosts') as f:
        nodelist = f.readlines()

    all_data = {}
    for x_node in nodelist:
        node_data = {}
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(x_node.strip(), username='root', password='cisco123')
            stdin, stdout, stderr = ssh.exec_command('netstat -s -t')
            for line in stdout.read().split('\n'):
                if 'resets sent' in line:
                    node_data['resets'] = int(line.strip(' ').split()[0])
                if 'segments retransmited' in line:
                    node_data['segments'] = int(line.strip(' ').split()[0])

            stdin, stdout, stderr = ssh.exec_command('ifconfig eth0')
            for line in stdout.read().split('\n'):
                if 'RX packets' in line:
                    node_data['rx_drop'] = line.strip().split()[3][8:]


                if 'TX packets' in line:
                    node_data['tx_drop'] = line.strip().split()[3][8:]

                if 'RX bytes' in line:
                    node_data['rx_bytes'] = line.strip().split()[1][6:]
                    node_data['tx_bytes'] = line.strip().split()[5][6:]
                    #print 'Rx Bytes/ Tx Bytes-', node_data['rx_bytes'], node_data['tx_bytes']

            stdin, stdout, stderr = ssh.exec_command('iostat')
            scan_flag = False
            sum_kb_read, sum_kb_wrtn = 0, 0
            for line in stdout.read().split('\n'):

                if line.strip() =='':
                    continue

                if 'Device:' in line:
                    scan_flag = True
                    continue

                if not scan_flag:
                    continue

                iostat_data = line.split()
                sum_kb_read += int(iostat_data[4])
                sum_kb_wrtn += int(iostat_data[5])

            node_data['sum_kb_read'] = sum_kb_read
            node_data['sum_kb_wrtn'] = sum_kb_wrtn


        except Exception as e:
            print 'ssh Connection Exception -', x_node.strip(), sys.exc_info()
            continue

        all_data[x_node.strip()] = node_data
    #print 'netstat -', json.dumps(all_data)
    return all_data
