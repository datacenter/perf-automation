import os, sys
import requests
import json
import time
import paramiko
import threading
from utility import *

dynamic_data = {}

def getDiskData(ssh):
    data = {}
    disks = []
    all_disks = []

    #Discover all disks, or just use the provided list.
    disks = getAllDisks(ssh)

    #Get the various disk data per disk.
    for disk in disks:
        diskDict = {}
        stdin, stdout, stderr = ssh.exec_command("iostat -m " + disk + " 2 1")
        ioStatData = stdout.read().split("\n")[6]
        diskDict["Disk_name"] = disk

        try:
            diskDict["Disk_Rd(MB)"] = long(ioStatData.split()[4])
        except:
            diskDict["Disk_Rd(MB)"] = 0

        try:
            diskDict["Disk_Wr(MB)"] = long(ioStatData.split()[5])
        except:
            diskDict["Disk_Wr(MB)"] = 0

        #print 'Disk Data :', diskDict
        all_disks.append(diskDict)

    #data["disk_stats"] = all_disks
    return all_disks

def getAllDisks(ssh):
    disks = []
    stdin, stdout, stderr = ssh.exec_command("ls -1 /sys/block | grep -E sd")
    for disk in stdout.read().split("\n"):
        if len(disk) > 0:
            disks = disks + [disk]
    return disks

def getIFaceStats(ssh, iface):    
    #ipLinkData = subprocess.check_output(["ssh", sshServerString, "ip -s link"])
    stdin, stdout, stderr = ssh.exec_command("ip -s link")
    ipLinkData = stdout.read().split("\n") 
    index = 0
    while index < len(ipLinkData):
        ipLinkName = ipLinkData[index].split(":")[1].strip()
        if ipLinkName == iface:
            RX = int(ipLinkData[index+3].split()[0].strip())
            TX = int(ipLinkData[index+5].split()[0].strip())
            return (RX,TX)
        else:
            index += 6
    print 'Error: Interface not found'
    return (0,0)

def getInterfaceData(ssh):
    interfaces = getAllInterfaces(ssh)
    returnList = []
    for interface in interfaces:
        if interface not in 'lo':
            interfaceDict = {}
            interfaceDict["Intr_Name"] = interface

            #IPAddressData = subprocess.check_output(["ssh", sshServerString, "ifconfig", interface])
            stdin, stdout, stderr = ssh.exec_command("ifconfig " + interface)        
            IPAddress = stdout.read().split("\n")[1].strip().split()[1]
            interfaceDict["IP_Addr"] = IPAddress

            #MACAddressData = subprocess.check_output(["ssh", sshServerString, "ethtool -P", interface])
            stdin, stdout, stderr = ssh.exec_command("ethtool -P " + interface)        
            MACAddress = stdout.read().split(": ")[1].strip()
            interfaceDict["MAC_Addr"] = MACAddress

            #DStatData = subprocess.check_output(["ssh", sshServerString, "dstat -N", interface, "2 1"])
            stdin, stdout, stderr = ssh.exec_command("dstat -N " + interface + " 2 1")        
            DStatData = stdout.read().split("\n")[4].split("|")[2].split()
            recv = DStatData[0]
            send = DStatData[1]
            send_num =0.00
            recv_num =0.00
            if(send[-1:].upper() =='B'):
                send_num = float(send[:-1])
                send_num = (float(send_num)/1000000)
            elif(send[-1:].upper() =='K'):
                send_num = float(send[:-1])/1000
            elif(send[-1:].upper() =='M'):
                send_num = float(send[:-1])
            else:
                send_num = 0

            if(recv[-1:].upper() =='B'):
                recv_num = float(recv[:-1])/1000000
            elif(recv[-1:].upper() =='K'):
                recv_num = float(recv[:-1])/1000
            elif(recv[-1:].upper() =='M'):
                recv_num = float(recv[:-1])
            else:
                recv_num = 0

            send_num = float("{0:.2f}".format(send_num))
            recv_num = float("{0:.2f}".format(recv_num))
            interfaceDict["NIC_Rx(Mb)"] = float(recv_num)*8
            interfaceDict["NIC_Tx(Mb)"] = float(send_num)*8

            TotalRcvSend = getIFaceStats(ssh, interface)

            interfaceDict["Cumulative_Receive(MB)"] = TotalRcvSend[0]/1000000
            interfaceDict["Cumulative_Send(MB)"] = TotalRcvSend[1]/1000000
            returnList = returnList + [interfaceDict]

    #data["Net_Util"] = returnList
    #print 'Interface Data -', returnList
    return returnList

def getAllInterfaces(ssh):
    interfaceList = []
    stdin, stdout, stderr = ssh.exec_command("ip link | awk '{print $2}'")
    interfaces = stdout.read().split("\n")

    i = 0
    for interface in interfaces:
        i+=1
        if (i%2 == 1):
            interfaceName = interface[:-1]

        try:
            #Data = subprocess.check_output(["ssh", sshServerString, "ifconfig | grep -E ", interfaceName])
            stdin, stdout, stderr = ssh.exec_command("ifconfig | grep -E " + interfaceName)        
            data = stdout.read().split("\n")[0]
            interfaceList = interfaceList + [interfaceName]
        except:
            interfaceList = interfaceList
    return interfaceList

def get_dynamic_data(ssh):
    data = {}
    disks, Interfaces = "all", "eth1 eth2"

    # Get CPU Data
    stdin, stdout, stderr = ssh.exec_command('vmstat 1 2')
    CPUData = stdout.read()
    CPUData = CPUData.split("\n")[3].split()
    IdleCPU = int(CPUData[14])
    data["CPU_Util(%)"] = 100 - IdleCPU
    
    # Get Memory Data
    stdin, stdout, stderr = ssh.exec_command('free -m')
    MemData = stdout.read()
    MemData = MemData.split("\n")[1].split()
    TotalMem = int(MemData[1])
    UsedMem = int(MemData[2])
    MemUtilization = (float(UsedMem)/TotalMem) * 100
    data["Mem_Util(%)"] = MemUtilization

    # Get Disk Data
    data['disk_stats'] = getDiskData(ssh) 
    data['Net_Util'] = getInterfaceData(ssh)
    return data 


def get_node_data_2(xnode):
    data = {}
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(xnode.strip(), username='root', password='cisco123')
        
        # Get Baremetal Data
        data = {}
        data = get_dynamic_data(ssh)
        
        global dynamic_data
        dynamic_data[xnode] = data

    except Exception as e:
        print 'ssh Connection Exception -', xnode.strip(), sys.exc_info()

    return

def get_node_dynamic_details(inputTag):
    nodelist = []
    with open(os.getcwd() + os.path.sep + 'hosts') as f:
        nodelist = f.readlines()

    iteration = 0
    while True:
        config, threads = {}, []
        with open(os.getcwd() + os.path.sep + "switchElastic.json") as configFile:
            config = json.load(configFile)

        if not config['dynamic_monitor']:
            print 'Monitoring not Configured'
            break

        for x_node in nodelist:
            t = threading.Thread(target=get_node_data_2, args=(x_node.strip(),))
            threads.append(t)

        start_time = time.time()
        iteration += 1
        print "\nDynamic Data Iteration - ", iteration

        global dynamic_data
        dynamic_data = {}
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        end_time = time.time()
        print '\nExecution Time :', end_time - start_time
        update_ESC('http://172.25.187.223:9200/' + 'monitorswitch/node_stat', None, inputTag, [ 'stat' ], [ dynamic_data ])

    print 'Stopping the Dynamic Server Data Monitoring.'
    return

if __name__ == "__main__":
    get_node_dynamic_details(sys.argv[1])
    sys.exit(0)
