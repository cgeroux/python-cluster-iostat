#!/usr/bin/env python
from __future__ import print_function
import optparse as op
from subprocess import Popen, PIPE
import os
import datetime
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt

def parseOptions():
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog [options] NODEFILE0 NODEFILE1 ..."
    ,version="%prog 1.0",description="")
  
  #parse command line options
  return parser.parse_args()
def runCmdOnHost(*args,**kwargs):
  if len(args)<2:
    raise Exception("must have a hostname and at least one command to run")
  
  #get wait keyword argument to indicate if we should wait for command to
  #complete and return stdout and stderr
  wait=True#by default wait
  if "wait" in kwargs.keys():
    wait=kwargs["wait"]
  
  #construct command
  if not wait:
    cmd="ssh "+args[0]+" \"nohup "
    for arg in args[1:]:
      cmd+=arg+" "
    cmd+=" &\""
  else:
    cmd=["ssh"]
    for arg in args:
      cmd.append(arg)
  
  if wait:
    process=Popen(cmd,stdout=PIPE,stderr=PIPE)
    stdout,stderr=process.communicate()
  else:
    stdout=None
    stderr=None
    Popen(cmd,stdout=PIPE,stderr=PIPE,shell=True)#output just dropped
  return (stdout,stderr)
def isIostatRunning(hostname):
  """Returns True if iostat is running under the current user otherwise
  returns False
  """
  
  (stdout,stderr)=runCmdOnHost(hostname,"pgrep", "-U", "$USER","iostat")
  if stdout:
    return True
  else:
    return False
def killAllIostats(hostname):
  """Kills all iostat processes running under the current user
  """
  
  #get process ID
  (stdout,stderr)=runCmdOnHost(hostname,"pkill", "-U", "$USER","iostat")
def removeAllLogs(hostname,device="/dev/vdb"):
  
  #remove logs in cwd
  for host in hostname:
    logFileName=makeLogFileName(host,device)
    
    #remove log file
    if os.path.isfile(logFileName):
      print("removing log file \""+logFileName+"\" ...")
      os.remove(logFileName)
    
    #remove plot
    logFileNameNoExt,ext=os.path.splitext(logFileName)
    plotFileName=logFileNameNoExt+".pdf"
    if os.path.isfile(plotFileName):
      print("removing plot file \""+plotFileName+"\" ...")
      os.remove(plotFileName)
  
  #remove logs on hosts
  for host in hostname:
    print(host+": removing log file ...")
    runCmdOnHost(host,"rm",logFileName)
def startIostat(hostname,device="/dev/vdb",interval=10):
  """Starts an iostat process monitoring the given device at the given interval
  of seconds
  """
  
  runCmdOnHost(hostname
    ,"iostat","-c","-d","-x","-t","-m",device,str(interval)
    ,">"+makeLogFileName(hostname,device)
    ,wait=False)
def makeLogFileName(hostname,device):
  dev=os.path.basename(device)
  return hostname+"-"+dev+"-iostat-log.txt"
def ensureIostatNotRunning(hostnames):
  """Kills iostats on each host listed in hostnames
  """
  
  for host in hostnames:
    
    print(host+": killing any running iostats ...")
    killAllIostats(host)
def ensureIostatRunning(hostnames,device="/dev/vdb",interval=10):
  """Checks each host in hostsnames list for a running iostat 
  process if there isn't one it starts one
  """
  
  for host in hostnames:
    
    #start iostat if it isn't already running
    if not isIostatRunning(host):
      print(host+": iostat not running, starting ...")
      startIostat(host,device=device,interval=interval)
    else:
      print(host+": iostat already running, doing nothing.")
def collectLogs(hostnames,device="/dev/vdb"):
  for host in hostnames:
    print("coping log from "+host+" ...")
    cmd=["scp",host+":"+makeLogFileName(host,device),"./"]
    process=Popen(cmd,stdout=PIPE,stderr=PIPE)
    stdout,stderr=process.communicate()
def readLog(iostatLog):
  """Reads the iostat log into a 2D list
    
    first dimension is time, the second dimension is a different measured
    quantity. In each "row" we have the python datetime object of the entry
    followed by different measured quantities. In order these quantities are:
    
    (1)%user: Show the percentage of CPU utilization that occurred while
      executing at the user level (application).
    (2)%nice: Show the percentage of CPU utilization that occurred while
      executing at the user level with nice priority.
    (3)%system: Show the percentage of CPU utilization that occurred while
      executing at the system level (kernel).
    (4)%iowait: Show the percentage of time that the CPU or CPUs were idle
      during which the system had an outstanding disk I/O request.
    (5)%steal: Show the percentage of time spent in involuntary wait by the 
      virtual CPU or CPUs while the hypervisor was servicing another virtual
      processor.
    (6)%idle: Show the percentage of time that the CPU or CPUs were idle and
      the system did not have an outstanding disk I/O request.
    (7)rrqm/s: The number of read requests merged per second that were queued
      to the device.
    (8)wrqm/s: The number of write requests merged per second that were queued
      to the device.
    (9)r/s: The number (after merges) of read requests completed per second for
      the device.
    (10)w/s: The number (after merges) of write requests completed per second
      for the device.
    (11)rMB/s: The number of sectors (kilobytes, megabytes) read from the
      device per second.
    (12)wMB/s: The number of sectors (kilobytes, megabytes) written to the
      device per second.
    (13)avgrq-sz: The average size (in sectors) of the requests that were 
      issued to the device.
    (14)avgqu-sz: The average queue length of the requests that were issued to
      the device.
    (15)await:The average time (in milliseconds) for I/O requests issued to 
      the device to be served. This includes the time spent by the requests in
      queue and the time spent servicing them.
    (16)r_await
    (17)w_await
    (18)svctm
    (19)%util
  """
  
  #first line describes the system a little
  f=open(iostatLog)
  header=f.readline()
  data=[]
  
  
  tmp=f.readline()#skip empty line
  
  #save date line
  dateTimeStr=f.readline()
  
  
  tmp=f.readline()#skip cpu header
  cpuUsage=f.readline()
  
  tmp=f.readline()#skip empty line
  tmp=f.readline()#skip device header
  deviceUsage=f.readline()
  
  if (not cpuUsage) or (not dateTimeStr) or (not deviceUsage):
    return data
  
  dateTime=datetime.datetime.strptime(dateTimeStr.strip(),"%m/%d/%Y %I:%M:%S %p")
  
  data.append([dateTime])
  for item in cpuUsage.split():
    data.append([float(item)])
  for item in deviceUsage.split()[1:]:
    data.append([float(item)])
  
  while True:
    
    tmp=f.readline()#skip empty line
    
    #save date line
    dateTimeStr=f.readline()
    
    
    tmp=f.readline()#skip cpu header
    cpuUsage=f.readline()
    
    tmp=f.readline()#skip empty line
    tmp=f.readline()#skip device header
    deviceUsage=f.readline()
    
    if (not cpuUsage) or (not dateTimeStr) or (not deviceUsage):
      break
    
    dateTime=datetime.datetime.strptime(dateTimeStr.strip(),"%m/%d/%Y %I:%M:%S %p")
    
    data[0].append(dateTime)
    count=1
    for item in cpuUsage.split():
      data[count].append(float(item))
      count+=1
    for item in deviceUsage.split()[1:]:
      data[count].append(float(item))
      count+=1
  return data
def plotLog(data,filename,y=12):
  plt.plot(data[0],data[y])
  plt.gcf().autofmt_xdate()
  plt.savefig(filename+".pdf")
  plt.close()
def plotLogs(hostnames,device="/dev/vdb"):
  for host in hostnames:
    logFileName=makeLogFileName(host,device)
    logFileNameNoExt,ext=os.path.splitext(logFileName)
    print("plotting log to file \""+logFileNameNoExt+".pdf\"")
    data=readLog(logFileName)
    plotLog(data,logFileNameNoExt)
def main():
  
  #parse command line options
  (options,args)=parseOptions()
  
  if len(args)<1:
    raise Exception("Expected to get a path to a file containing a list of "
      +"nodes")
  
  #get a list of nodes
  nodesHostNames=[]
  for path in args:
    f=open(path)
    for line in f:
      nodesHostNames.append(line.strip())
  
  #1) stop logging and leave logs
  #ensureIostatNotRunning(nodesHostNames)
  
  #2) stop logging and remove logs
  #ensureIostatNotRunning(nodesHostNames)
  #removeAllLogs(nodesHostNames)
  
  #3) start logging
  #ensureIostatRunning(nodesHostNames)
  
  #4) collect logs and plot
  collectLogs(nodesHostNames)
  plotLogs(nodesHostNames)
  
if __name__ == "__main__":
  main()