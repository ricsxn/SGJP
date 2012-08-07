#!/usr/bin/env python
#
# SGJP_client.py - Science Gateway Job Perusal client service 
#
# To activate the application job perusal the application pilot script has to include
# the following statement:
#
# python <(curl http://<SGJP_server_host>:<SGJP_server_port>/sgjp 2>/dev/null) <app_id> '<user_name>' '<job description>' &
#
# The above call is used by the 'sgjp_start' shell function that will start a child 
# process whilt it will be terminated by the 'sgjp_end' function. Both functions could be
# defined inside the job pilot script as:
#
#sgjp_start()
#{
#	APPID=$1
#   JOBDES=$2
#	echo "Starting SGJP"
#	python <(curl http://<SGJP_server_host>:<SGJP_server_port>/sgjp 2>/dev/null) $APPID '$JOBDESC' &
#	SGJP_PID=$!
#}
#
# The client code recognize the parent closure; anyway the client can be stopped anytime by:
#
#sgjp_end()
#{
#	kill $SGJP_PID && wait $SGJP_PID
#}
#
# Riccardo Bruno (riccardo.bruno@ct.infn.it)
#

# !! Multi server (not yet implemented)
# -------------------------------------
# The server and port name are assigned at run-time by the application server
# The application server owns a list of job perusal servers and before to execute
# the job will determine the less busy server quering all of them with the 'loud' 
# query. The selected server host/port will be input values for the pilot script
#
# <pilot_script> <sgjp_server_host> <sgjp_server_port> <sg_jobdescription>
#
import os,sys,signal,random
import urllib, urllib2
from xml.dom.minidom import parseString

sgjp_server_host    = '<SERVER_HOST>' 
sgjp_server_port    = <SERVER_PORT>
sgjp_server_loop    = True
sgjp_server_counter = 1
sgjp_server_cycles  = 0
sgjp_hbeat_time     = 1

# Application file information, stores information kept in SGJP table: sgjp_application_files 
class AppFile:
	file_id   = 0
	file_path = ""
	file_step = 0
	file_binary = False
	# tracking params
	file_size = 0

# AppInfo
# -------
# Application information stores information kept in SGJP table: sgjp_applications
# it also stores information of default tracked files as list of AppFile objects
class AppInfo:
	def __init__(self):
		self.is_valid         = False
		self.app_id           = -1
		self.app_files        = []
		self.app_files_map    = {}
		self.app_name         = ""
		self.app_default_step = ""
		self.app_desc         = ""
		self.job_uname        = ''

	def __init__(self,app_id,job_uname,app_desc):
		self.is_valid         = False
		self.app_id           = app_id
		self.app_files        = []
		self.app_files_map    = {}
		self.app_name         = ""
		self.app_default_step = ""
		self.app_desc         = app_desc
		self.job_uname        = job_uname
		print 'Getting info for application having identifier: \'%d\'' % self.app_id
		self.LoadAppInfo()

	# Retrieving application infromation stored into the database via REST queries
	# the server address is automatically provided by the Application server
	def LoadAppInfo(self):
		try:
			print "<appinfo> app_id: %s" %self.app_id
			rest_query = 'http://%s:%s/appinfo/%d' % (sgjp_server_host,sgjp_server_port,self.app_id)
			print rest_query
			data       = urllib2.urlopen(rest_query).read()
			dom        = parseString(data)
			xml_error  = dom.getElementsByTagName('error')
			if(xml_error!=[]):
				sql_message = xml_error[0].getAttribute("sqlerr")
				print "ERROR : '%s'"%err_message
				print "SQLERR: '%s'"%sql_message
			else:
				xml_application       = dom.getElementsByTagName('application')
				xml_app_tag           = xml_application[0]
				self.app_name         = xml_app_tag.getAttribute("name")
				self.app_default_step = xml_app_tag.getAttribute("default_step")
				self.app_id=xml_app_tag.getAttribute("id")
				xml_files = dom.getElementsByTagName('file')
				for xml_file in xml_files:
					app_file             = AppFile()
					app_file.file_id     = int(xml_file.getAttribute("id"))
					app_file.file_step   = int(xml_file.getAttribute("file_step"))
					app_file.file_path   = xml_file.getAttribute("file_path")
					app_file.file_binary = xml_file.getAttribute("file_binary")
					self.app_files       = self.app_files+[app_file]
				# Enable info flag
				self.is_valid = True
		except urllib2.HTTPError, e:
			print "HTTP error: %d" % e.code
		except urllib2.URLError, e:
			print "Network error: %s" % e.reason #.args[1]
			
	# Dump object
	def dump(self):
		print "AppInfo dump:"
		print "-------------"
		print "isValid=%s"        % self.is_valid
		print "appId=%s"          % self.app_id
		print "appName='%s'"      % self.app_name
		print "appDefaultStep=%s" % self.app_default_step
		print "AppFiles:"
		for file in self.app_files:
			print "\tid: %s - step: %s - path: '%s'" %(file.file_id,file.file_step,file.file_path)
		print ""

	def IsValid(self):
		return self.is_valid

# AppTracking
# ----------
# This class collects all data and methods related to the job tracking
class AppTracking:
	def __init__(self):
		self.is_valid     = False
		self.app_info     = None
		self.job_track_id = -1
		self.job_grid_id  = ''

	def __init__(self,app_info):
		self.is_valid     = False
		self.app_info     = app_info
		self.job_track_id = -1
		self.job_grid_id  = ''


	# function that generates a random string
	def genRndString(self,rndLength):
		alphabet = '01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
		min = 1
		max = 1
		rndString=''
		for count in xrange(1,rndLength):
			for c in random.sample(alphabet,random.randint(min,max)):
				rndString+=c
		return rndString
	
	# This function determines the current grid job identifyer
	# The LB key value can be obtained from os.environ['<env_var>'][-22:]
	def getGridJobId(self):
		try:
			job_grid_id=os.environ['GRID_JOBID']
		except KeyError, k:
			try:
				job_grid_id=os.environ['GLITE_WMS_JOBID']
			except KeyError, k:
				try:
					job_grid_id=os.environ['EDG_WL_JOBID']
				except KeyError, k:
					job_grid_id="https://no-lb:no-port/"+self.genRndString(22)
					print "Warning: Unable to retrieve the original job identifyer; new id: %s'" % self.job_grid_id
		return job_grid_id
	
	# This method registers the job tracking operation into the SGJP server sending the jobId
	# Actions performed by this call:
	#	record insertion into table: sgjp_tracked_jobs
	#	filling default files to track in table: sgjp_job_files
	def registerJob(self):
		# Retrieves the jobId
		self.job_grid_id = self.getGridJobId()
		print "<registerjob> job_grid_id: '%s' for tracking id: '%s'" % (self.job_grid_id,self.app_info.app_id)
		rest_query='http://%s:%s/register' % (sgjp_server_host,sgjp_server_port)
		#print 'register rest query: %s' % rest_query
		#,self.app_info.app_id,urllib.quote(self.job_grid_id),self.app_info.job_uname,urllib.quote(self.app_info.app_desc))
		try:
			values = [ ('app_id'   , '%d' % int(self.app_info.app_id))
			          ,('job_id'   , '%s' % self.job_grid_id         )
			          ,('job_uname', '%s' % self.app_info.job_uname  )
			          ,('job_desc' , '%s' % self.app_info.app_desc   ) ]
			data = urllib.urlencode(values)
			req = urllib2.Request(rest_query)
			data = urllib2.urlopen(rest_query,data).read()
			dom = parseString(data)
			xml_error  = dom.getElementsByTagName('error')
			if(xml_error!=[]):
				err_message = xml_error[0].getAttribute("message")
				sql_message = xml_error[0].getAttribute("sqlerr")
				print "ERROR : '%s'"%err_message
				print "SQLERR: '%s'"%sql_message
			else:
				xml_track = dom.getElementsByTagName('track')
				xml_track_tag  = xml_track[0]
				self.job_track_id = xml_track_tag.getAttribute("id")
				print "id: %s" % self.job_track_id
				if int(self.job_track_id) > 0:
					self.is_valid=True
		except urllib2.HTTPError, e:
			print "HTTP error: %d" % e.code
		except urllib2.URLError, e:
				print "Network error: %s" % e.reason.args[1]
				
		
	def registerFiles(self):
		for file in self.app_info.app_files:
			print "<registerfile> job_track_id: %s - id: %s - path: '%s' - step: %s" %(self.job_track_id,file.file_id,file.file_path,file.file_step)
			rest_query='http://%s:%s/register_file' % (sgjp_server_host,sgjp_server_port)
			#print 'registerfile rest query: %s' % rest_query
			#,self.job_track_id,file.file_id,file.file_path,file.file_step)
			try:
				values = [ ('job_track_id', '%d' % int(self.job_track_id))
				          ,('file_id'     , '%s' % file.file_id)
				          ,('file_path'   , '%s' % file.file_path)
				          ,('file_step'   , '%s' % file.file_step)
				          ,('file_binary' , '%s' % file.file_binary) ]
				data = urllib.urlencode(values)
				req = urllib2.Request(rest_query)
				data = urllib2.urlopen(rest_query,data).read()
				dom = parseString(data)
				xml_error  = dom.getElementsByTagName('error')
				if(xml_error!=[]):
					err_message = xml_error[0].getAttribute("message")
					sql_message = xml_error[0].getAttribute("sqlerr")
					print "ERROR : '%s'"%err_message
					print "SQLERR: '%s'"%sql_message
				else:
					xml_track = dom.getElementsByTagName('result')
					xml_track_tag  = xml_track[0]
					result = xml_track_tag.getAttribute("status")
					if result != 'OK':
						self.is_valid=False
			except urllib2.HTTPError, e:
				print "HTTP error: %d" % e.code
				self.is_valid=False
			except urllib2.URLError, e:
				print "Network error: %s" % e.reason.args[1]
				self.is_valid=False
	
	def jobInfo(self):
		print "Registering jobInfo"
		for param in os.environ.keys():
			try:
				print "<jobinfo> param: %s - value: '%s' " %(param,os.environ[param])
				rest_query='http://%s:%s/jobinfo' % (sgjp_server_host,sgjp_server_port)
				#print 'jobinfo rest query: %s' % rest_query
				values = [ ('job_track_id' , '%d' % int(self.job_track_id))
				          ,('info_name'    , '%s' % param)
				          ,('info_value'   , '%s' % os.environ[param]) ]
				data = urllib.urlencode(values)
				req = urllib2.Request(rest_query)
				data = urllib2.urlopen(rest_query,data).read()
				dom = parseString(data)
				xml_error  = dom.getElementsByTagName('error')
				if(xml_error!=[]):
					err_message = xml_error[0].getAttribute("message")
					sql_message = xml_error[0].getAttribute("sqlerr")
					print "ERROR : '%s'"%err_message
					print "SQLERR: '%s'"%sql_message
				else:
					xml_track = dom.getElementsByTagName('result')
					xml_track_tag  = xml_track[0]
					result = xml_track_tag.getAttribute("status")
			except urllib2.HTTPError, e:
				print "HTTP error: %d" % e.code
				self.is_valid=False
			except urllib2.URLError, e:
				print "Network error: %s" % e.reason.args[1]
				self.is_valid=False

	# This method check all files to monitor and register the snaphots if necessary
	def checkfiles(self):
		global sgjp_server_counter
		files_map=self.app_info.app_files_map
		for file in self.app_info.app_files:
			if (sgjp_server_counter%file.file_step) == 0:
				print "<checkfile> id: %s - path: '%s' - step: %s" %(file.file_id,file.file_path,file.file_step)
				# File path may contain wildcards: '*', '?'
				# In such case a further loop is requested
				if '*' in file.file_path or \
				   '?' in file.file_path:
					f=os.popen("/bin/ls -1 %s" % file.file_path)
					file_path_list=()
					for file_path in f.readlines():
						file_path_list=file_path_list+(file_path,)
						try:
							file_map_record=files_map[file_path]
						except:
							files_map[file_path]=0
				else:
					file_path_list=(file.file_path,)
					try:
						file_map_record=files_map[file.file_path]
					except:
						files_map[file.file_path]=0
				# Now process files in the list 'file_list'
				# The size change detection uses files_map[file_path]
				for file_path in file_path_list:
					try:
						new_file_size=os.path.getsize(file_path)
						print "  file: %s - old_sz: %s - new_sz: %s" %(file_path,files_map[file_path],new_file_size)
						if new_file_size != files_map[file_path]:
							# File changes detected; sending the related snapshot
							fo = open(file_path, "r")
							fo.seek(files_map[file_path],0)
							snapshot_content=fo.read()
							fo.close()
							files_map[file_path]=new_file_size
							# Only wildcarded files stores file_path value
							# into the snapshot records
							if '*' in file.file_path or \
							   '?' in file.file_path:
								snapshot_file_path=file_path
							else:
								snapshot_file_path=''
							# Snapshot content can be sent to the server (POST)
							try:
								rest_query='http://%s:%s/send_snapshot' % (sgjp_server_host,sgjp_server_port)
								print 'jobinfo rest query: %s' % rest_query
								values = [ ('job_track_id'    , '%d' % int(self.job_track_id))
								          ,('file_id'         , '%d' % int(file.file_id))
								          ,('snapshot_content', '%s' % snapshot_content)
								          ,('file_binary'     , '%s' % file.file_binary)
								          ,('file_path'       , '%s' % snapshot_file_path) ]
								data = urllib.urlencode(values)
								req = urllib2.Request(rest_query)
								data = urllib2.urlopen(rest_query,data).read()
								dom = parseString(data)
								xml_error  = dom.getElementsByTagName('error')
								if(xml_error!=[]):
									err_message = xml_error[0].getAttribute("message")
									sql_message = xml_error[0].getAttribute("sqlerr")
									print "ERROR : '%s'"%err_message
									print "SQLERR: '%s'"%sql_message
								else:
									xml_track = dom.getElementsByTagName('result')
									xml_track_tag  = xml_track[0]
									result = xml_track_tag.getAttribute("status")
							except urllib2.HTTPError, e:
								print "HTTP error: %d" % e.code
								self.is_valid=False
							except urllib2.URLError, e:
								print "Network error: %s" % e.reason.args[1]
								self.is_valid=False
					except OSError:
						print "The file: '%s' does not exist" % file.file_path
		sgjp_server_counter=sgjp_server_counter+1
		
	# This method will be called before to close the job tracking
	def close(self):
		print "<close> job_track_id: '%s'" % self.job_track_id
		rest_query='http://%s:%s/close/%s' % (sgjp_server_host,sgjp_server_port,self.job_track_id)
		#print 'Register rest query: %s' % rest_query
		try:
			data = urllib2.urlopen(rest_query).read()
			dom = parseString(data)
			xml_error  = dom.getElementsByTagName('error')
			if(xml_error!=[]):
				err_message = xml_error[0].getAttribute("message")
				sql_message = xml_error[0].getAttribute("sqlerr")
				print "ERROR : '%s'"%err_message
				print "SQLERR: '%s'"%sql_message
			else:
				xml_track = dom.getElementsByTagName('result')
				xml_track_tag  = xml_track[0]
				result = xml_track_tag.getAttribute("status")
		except urllib2.HTTPError, e:
			print "HTTP error: %d" % e.code
		except urllib2.URLError, e:
				print "Network error: %s" % e.reason.args[1]
		
	def dump(self):
		print "AppTracking:"
		print "------------"
		print "job_track_id: %s" % self.job_track_id
		
	def IsValid(self):
		return self.is_valid

# Show client usage
def showUsage():
	print "Usage: SGJP_client <app_id> <username> <job_description>"

#----------------------------------------------
# Main method; the job perusal starts here ... 
#----------------------------------------------
def main(argv=None):
	global sgjp_server_loop
	global sgjp_server_counter
	
	# The function requires as parameter the application id
	argc = len(argv)
	if argc == 0:
		print 'No job identifier given; unable to perform the job tracking'
		showUsage()
		return 10
	elif argc < 3:
		print 'Wrong number of arguments'
		showUsage()
		return 20
		
	# Startup the tracking process ...
	app_id   = int(argv[0])
	job_uname=     argv[1]
	app_desc =     argv[2]
	
	print "SGJP - Client"
	print "-------------"
	print "SGJP Host: '%s'" % sgjp_server_host
	print "SGJP Port: '%s'" % sgjp_server_port
	print "AppId    : '%d'" % app_id
	print "User     : '%s'" % job_uname
	print "Job desc.: '%s'" % app_desc
	print ""
	
	# Application information will be retrieved by the server
	app_info=AppInfo(app_id,job_uname,app_desc)
	app_info.dump()
	if app_info.IsValid() != True:
		print 'No valid application information available; unable to perform job tracking'
		return 30
	# Try to register the new tracking process into the server sending the job identifier
	app_tracking=AppTracking(app_info)
	app_tracking.registerJob()
	app_tracking.registerFiles()
	app_tracking.dump()
	if app_tracking.IsValid() != True:
		print 'Unable to register job tracking into the server'
		return 40
	
	# Fill job information
	app_tracking.jobInfo()
	
	# Start the main loop
	print "SGJP entering in the monitoring loop waiting for a CTLRL+C"
	print "or a explicit kill call done by the parent process"
	print "The loop fires a timer handler each (1 sec step)"
	signal.signal(signal.SIGALRM, signal_handler_HBeat)
	
	ppid=os.getppid()
	while sgjp_server_loop:
		signal.alarm(sgjp_hbeat_time)
		#call the job perusal main routine
		app_tracking.checkfiles()
		signal.pause()
		# Checks if the parent process still exists
		if os.getppid() != ppid:
			# If the parent process does not exist anymore exit from the loop
			sgjp_server_loop = False
			
	# Update snapshots and Close the tracking
	print "Exited from mail loop ..."
	print "Client cycled: '%s' times" % sgjp_server_cycles
	print "Each cycle took: '%s' seconds" % sgjp_hbeat_time
	sgjp_server_counter=0
	sys.stdout.flush()
	app_tracking.checkfiles()
	app_tracking.close()
	
	# Everithing went ok; successfull notification will be returned (0)
	return 0

# Following handler function ckecks the monitored files timely (1 sec based)
# As soon as one file step has been reached it start and register the related 
# file snapshot
def signal_handler_HBeat(signal, frame):
	global sgjp_server_cycles
	sgjp_server_cycles=sgjp_server_cycles+1
	#Call the proper methods ...

# Following handler function will be called when the monitoring service will be 
# stopped by the calling script. This method will register the end of the monitoring
# service into the db
def signal_handler_Term(signal, frame):
	global sgjp_server_loop
	print 'Terminating!'
	sgjp_server_loop=False
	

# Following handler function will be called when the monitoring service will be 
# stopped by the calling script. This method will register the end of the monitoring
# service into the db
def signal_handler_Int(signal, frame):
	global sgjp_server_loop
	print '^C Interruption requested'
	sgjp_server_loop=False

# Main code call
if __name__ == "__main__":
	# Register the signal handlers
	signal.signal(signal.SIGTERM, signal_handler_Term)
	signal.signal(signal.SIGINT , signal_handler_Int)
	sys.exit(main(sys.argv[1:]))
