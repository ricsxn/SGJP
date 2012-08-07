#!/bin/env python 
#
# SGJP_server.py - Science Gateway Job Perusal server
#
# Riccardo Bruno (riccardo.bruno@ct.infn.it)
#
import os
import sys
import tempfile
import base64
import smtplib
import MySQLdb
import ConfigParser
from flask import Flask
from flask import render_template
from flask import request
from flask import Response
from xml.dom.minidom import Document 
from email import Encoders
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formatdate

# Load SGJP Server settings
class Config:
	def __init__(self):
		config = ConfigParser.ConfigParser()
		config.read('SGJP.cfg')
		self.sgjp_hostname    = config.get('server','sgjp_hostname'   )
		self.sgjp_port        = config.get('server','sgjp_port'       )
		self.sgjp_db_host     = config.get('server','sgjp_db_host'    )
		self.sgjp_db_user     = config.get('server','sgjp_db_user'    )
		self.sgjp_db_password = config.get('server','sgjp_db_password')
		self.sgjp_db_name     = config.get('server','sgjp_db_name'    )
		self.showConf()
		
	def showConf(self):
		print "SGJP - Server"
		print "-------------"
		print "Configuration settings:"
		print "  Host         : '%s'" % self.sgjp_hostname
		print "  Port         : '%s'" % self.sgjp_port
		print "  Database host: '%s'" % self.sgjp_db_host
		print "  Database user: '%s'" % self.sgjp_db_user
		print "  Database pass: '%s'" % self.sgjp_db_password
		print "  Database name: '%s'" % self.sgjp_db_name


# Initialize the Flask application
app = Flask(__name__)

@app.route('/')
def Index():
	return 'SGJP - Science Gateway Job Perusal'

# Loud service returns an integer value (at the beginning just the number of active tracking)
# so that the job dispatching service can address the less busy SHGJP server
# This method is not implemented yet
@app.route("/loud", methods=['GET','POST'])
def Loud():
	 if request.method == 'POST':
	 	return 'Loud in POST mode'
	 elif request.method == 'GET':
	 	#A trivial load balancing can be obtained just returning the number of 
	 	#active transactions
	 	#select count(*) from sgjp_tracked_jobs where end_ts is NULL;
	 	return 'Loud in GET mode'
	 else:
	 	return 'Unknown method'

# This call sends a mail notification to a given email address
@app.route("/notify", methods=['GET','POST'])
def Notify():
	if request.method == 'POST':
		# Retrieve values
		email_from = request.values.get('email_from')
		email_to   = request.values.get('email_to'  )
		email_subj = request.values.get('email_subj')
		email_body = request.values.get('email_body')
		email_logo = request.values.get('email_logo')
		print "<notify> email_from: '%s' - email_to: '%s' - email_subj: '%s' - email_body_size: %s - email_logo_size: %s" %(email_from,email_to,email_subj,len(email_body),len(email_logo))
		#print "--[email_body content (begin)]-------------------------------"
		#print email_body
		#print "--[email_body content (end)  ]-------------------------------"
		#print "--[email_logo content (begin)]-------------------------------"
		#print email_logo
		#print "--[email_logo content (end)  ]-------------------------------"
		# Prepare the answer XML
		doc = Document()
		# Send the email
		msg = MIMEMultipart('related')
		msg["From"]    = email_from
		msg["To"]      = email_to
		msg["Subject"] = "SGJP: %s" % email_subj
		msg['Date']    = formatdate(localtime=True)
		# html body
		html="<html><body>"+email_body+"<br/><small>SGJP server: (%s)</small><br/></body></html>" % sgjp_hostname
		mhtml_part=MIMEText(html, 'html')
		msg.attach(mhtml_part) 
		# attach the logo
		unencoded_logo=base64.b64decode(email_logo)
		part = MIMEBase('application', "octet-stream")
		part.set_payload(unencoded_logo)
		Encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="AppLogo.png"')
		part.add_header('Content-ID', '<image1>')
		msg.attach(part)
		server = smtplib.SMTP('localhost')
		# server.login(username, password)  # optional 
		try:
			failed = server.sendmail(email_from, email_to, msg.as_string())
			server.close()
			doc_track = doc.createElement("result")
			doc_track.setAttribute("status", "OK")
			doc.appendChild(doc_track)
		except Exception, e:
			print 'exception!!!'
			print repr(e)+"\n"
			doc_error = doc.createElement("error")
			doc.appendChild(doc_error)
			doc_error.setAttribute("message", "Unable to send notification from: '%s' - to: '%s' - subject: '%s'" % (email_from,email_to,email_subj))
		#cherrypy.response.headers['Content-Type']= 'text/xml'
		return doc.toprettyxml(indent="\t")
	elif request.method == 'GET':
	 	return 'Please use this method with POST'
	else:
	 	return 'Unknown method requested'

# This sends the snapshot of a given job (job_track_id) for a given file (job_file_id)
# each snapshot is identified by its timestamp (now() call in the insert statement)
# This service only works with POST
@app.route("/send_snapshot", methods=['GET','POST'])
def SendSnapshot():
	if request.method == 'POST':
		try:
			# Retrieve values
			job_track_id     = request.values.get('job_track_id'    )
			file_id          = request.values.get('file_id'         )
			snapshot_content = request.values.get('snapshot_content')
			file_binary      = request.values.get('file_binary'     )
			file_path        = request.values.get('file_path'       )
			print "<send_snapshot> track_id: %s - file_id: %s - file_binary: %s - snapshot_size: %s - file_path: %s" %(job_track_id,file_id,file_binary,len(snapshot_content),file_path)
			#print "--[snapshot_content (begin)]-------------------------------"
			#print snapshot_content
			#print "--[snapshot_content (end)  ]-------------------------------"
			# Prepare the answer XML
			doc = Document()
			# Connect to the database
			db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
			cursor=db.cursor()
			# Get default file check step (seconds) and application name
			if file_binary == 'True':
				query = """insert into sgjp_snapshots (job_tracking_id,file_id,snapshot_ts,binary_content,file_path) values (%s,%s,now(),%s,%s);"""
			else:
				query = """insert into sgjp_snapshots (job_tracking_id,file_id,snapshot_ts,text_content,file_path) values (%s,%s,now(),%s,%s);"""
			params=(job_track_id,file_id,snapshot_content,file_path)
			# Execute the SQL command
			cursor.execute(query,params)
			doc_track = doc.createElement("result")
			doc_track.setAttribute("status", "OK")
			doc.appendChild(doc_track)
			db.close()
			#cherrypy.response.headers['Content-Type']= 'text/xml'
			return doc.toprettyxml(indent="\t")
		except MySQLdb.Error, e:
			print 'exception!!!'
			print repr(e)+"\n"
			# Rollback in case there is any error
			db.rollback()
			doc_error = doc.createElement("error")
			doc.appendChild(doc_error)
			doc_error.setAttribute("message", "Unable to send snapshot forjob tracking id: '%s' - file_id: '%s'" % (job_track_id,file_id))
			doc_error.setAttribute("sqlerr" , repr(e)+"\n"+query)
			#doc_error.setAttribute("query"  , query)
			db.close()
		sys.stdout.flush()
		#cherrypy.response.headers['Content-Type']= 'text/xml'
		return doc.toprettyxml(indent="\t")
	elif request.method == 'GET':
	 	return 'Please use this method with POST'
	else:
	 	return 'Unknown method requested'

# After the client gets the application information it will register the tracking operation
# into the database and the key job_tracking_id will be returned back to the client
# This service only works with POST
@app.route("/register",methods=['GET','POST']) #/<app_id>/<job_id>/<job_uname>/<job_desc>
def Register():
	if request.method == 'POST':
		try:
			# Retrieve values
			app_id    = request.values.get('app_id'   )
			job_id    = request.values.get('job_id'   )
			job_uname = request.values.get('job_uname')
			job_desc  = request.values.get('job_desc' )
			print "<register> app_id: %s - job_grid_id: '%s' - job_uname '%s' - job_desc '%s'" % (app_id,job_id,job_uname,job_desc)
			# Prepare the answer XML
			doc = Document()
			# Connect to the database
			db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
			cursor=db.cursor()
			# Get default file check step (seconds) and application name
			query="""insert into sgjp_tracked_jobs (app_id,job_grid_id,start_ts,job_uname,job_desc) values (%s,'%s',now(),'%s','%s');""" % (app_id,str(MySQLdb.escape_string(job_id)),job_uname,job_desc)
			# Execute the SQL command
			cursor.execute(query)
			# Retrieves back the new job tracking id
			query="select max(job_tracking_id) from sgjp_tracked_jobs where app_id=%s;" %app_id
			cursor.execute(query)
			track_id= cursor.fetchone()
			# Commit your changes in the database
			db.commit()	
			db.close()
			doc_track = doc.createElement("track")
			doc_track.setAttribute("id", "%s"%track_id)
			doc.appendChild(doc_track)
		except MySQLdb.Error, e:
			print 'exception!!!'
			# Rollback in case there is any error
			db.rollback()
			db.close()
			doc_error = doc.createElement("error")
			doc.appendChild(doc_error)
			doc_error.setAttribute("message", "Unable to register job tracking")
			doc_error.setAttribute("sqlerr", repr(e))
			#doc_error.setAttribute("query", query)
		sys.stdout.flush()
		#cherrypy.response.headers['Content-Type']= 'text/xml'
		return doc.toprettyxml(indent="\t")
	elif request.method == 'GET':
	 	return 'Please use this method with POST'
	else:
	 	return 'Unknown method requested'
	
# After client registers the application to track it will register each application file
# to monitor. Records having <file_id> == NULL are pointing to non standard files.
# In such cases a new file_id will be generated as (max(file_id)+1) related to the 
# current job_tracking_id
# This service only works with POST
@app.route("/register_file",methods=['GET','POST']) 
def RegisterFile():
	if request.method == 'POST':
		try:
			# Retrieve values
			job_track_id = request.values.get('job_track_id')
			file_id      = request.values.get('file_id'     )
			file_path    = request.values.get('file_path'   )
			file_step    = request.values.get('file_step'   )
			file_binary  = request.values.get('file_binary' )
			print "<register_file> job_track_id: %s - file_id: %s - file_path: '%s' - file_step: %s - file_binary: %s" %(job_track_id,file_id,file_path,file_step,file_binary)
			# Prepare the answer XML
			doc = Document()
			# Connect to the database
			db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
			cursor=db.cursor()
			# Get default file check step (seconds) and application name
			query="""insert into sgjp_job_files (job_tracking_id,file_id,file_path,file_step,file_binary) values (%s,%s,'%s',%s,%s);""" % (job_track_id,file_id,file_path,file_step,file_binary)		
			# Execute the SQL command
			cursor.execute(query)
			# Commit your changes in the database
			db.commit()	
			db.close()
			doc_track = doc.createElement("result")
			doc_track.setAttribute("status", "OK")
			doc.appendChild(doc_track)
		except MySQLdb.Error, e:
			print 'exception!!!'
			# Rollback in case there is any error
			db.rollback()
			db.close()
			doc_error = doc.createElement("error")
			doc.appendChild(doc_error)
			doc_error.setAttribute("message", "Unable to register job file id: '%s' - path: '%s' - step: '%s'" % (file_id,file_path,file_step))
			doc_error.setAttribute("sqlerr", repr(e)+"\n"+query)
			#doc_error.setAttribute("query", query)
		sys.stdout.flush()
		#cherrypy.response.headers['Content-Type']= 'text/xml'
		return doc.toprettyxml(indent="\t")
	elif request.method == 'GET':
	 	return 'Please use this method with POST'
	else:
	 	return 'Unknown method requested'
	
# After client registered the job tracking and each default file to track it will save 
# environment variables ans other information using this service
# This service only works with POST
@app.route("/jobinfo",methods=['GET','POST'])
def JobInfo():
	if request.method == 'POST':
		try:
			# Retrieve values
			job_track_id = request.values.get('job_track_id')
			info_name    = request.values.get('info_name'   )
			info_value   = request.values.get('info_value'  )
			print "<jobinfo> job_track_id: %s - info_name: %s - info_value: %s" %(job_track_id,info_name,info_value)
			# Prepare the answer XML
			doc = Document()
			# Connect to the database
			db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
			cursor=db.cursor()
			# Get default file check step (seconds) and application name
			query="""insert into sgjp_job_info (job_tracking_id,info_name,info_value) values (%s,'%s','%s');""" % (job_track_id,info_name,MySQLdb.escape_string(info_value))
			# Execute the SQL command
			cursor.execute(query)
			db.commit()
			db.close()
			doc_track = doc.createElement("result")
			doc_track.setAttribute("status", "OK")
			doc.appendChild(doc_track)
			#cherrypy.response.headers['Content-Type']= 'text/xml'
			return doc.toprettyxml(indent="\t")
		except MySQLdb.Error, e:
			print 'exception!!!'
			print repr(e)+"\n"
			# Rollback in case there is any error
			db.rollback()
			db.close()
			doc_error = doc.createElement("error")
			doc.appendChild(doc_error)
			doc_error.setAttribute("message", "Unable to register job info name: '%s' - value: '%s'" % (info_name,info_value))
			doc_error.setAttribute("sqlerr", repr(e)+"\n"+query)
			#doc_error.setAttribute("query", query)
		sys.stdout.flush()
		#cherrypy.response.headers['Content-Type']= 'text/xml'
		return doc.toprettyxml(indent="\t")
	elif request.method == 'GET':
	 	return 'Please use this method with POST'
	else:
	 	return 'Unknown method requested'

# When the job is terminating, the tracking operation will be closed regitering the 
# end_ts filed into the sgjp_tracked_jobs table. This informs that the job execution
# has been completed
# This service only works with GET
@app.route("/close/<job_track_id>")
def Close(job_track_id):
	try:
		print "<close> job_track_id: %s" %job_track_id
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Get default file check step (seconds) and application name
		query="""update sgjp_tracked_jobs set end_ts=now() where job_tracking_id=%s;""" % job_track_id
		# Execute the SQL command
		cursor.execute(query)
		db.commit()
		db.close()
		doc_track = doc.createElement("result")
		doc_track.setAttribute("status", "OK")
		doc.appendChild(doc_track)
	except MySQLdb.Error, e:
		print 'exception!!!'
		# Rollback in case there is any error
		db.rollback()
		db.close()
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to close tracking with id: '%s'" %job_tracking_id)
		doc_error.setAttribute("sqlerr", repr(e)+"\n"+query)
		#doc_error.setAttribute("query", query)
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	return doc.toprettyxml(indent="\t")

# This is the first service called by the SGJP service, it takes as input
# the application id stored into the server database and will extract
# application information and the default settings such as files to track and 
# the related step values
# This service only works with GET
@app.route("/appinfo/<app_id>")
def AppInfo(app_id):
	try:
		print "<appinfo> app_id: %s" %app_id
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Get default file check step (seconds) and application name
		query = """select app_name,app_default_step from sgjp_applications where app_id=%s;""" % app_id
		cursor.execute(query)
		results = cursor.fetchall()
		for row in results:
			app_name=row[0]
			app_default_step=row[1]
		# Put output into the XML
		doc_appinfo = doc.createElement("appinfo")
		doc.appendChild(doc_appinfo)
		doc_appinfo_application = doc.createElement("application")
		doc_appinfo_application.setAttribute("id", "%s"%app_id)
		doc_appinfo_application.setAttribute("name", app_name)
		doc_appinfo_application.setAttribute("default_step", "%s"%app_default_step)
		doc_appinfo.appendChild(doc_appinfo_application)
		# Get the list of files to check
		doc_appinfo_files = doc.createElement("files")
		doc_appinfo_application.appendChild(doc_appinfo_files)
		query = """select file_id, app_file_path, app_file_step,app_file_binary from sgjp_application_files where app_id=%s;""" % app_id
		cursor.execute(query)
		results = cursor.fetchall()
		for row in results:
			file_id=row[0]
			file_path=row[1]
			file_step=row[2] if row[2] is not None else app_default_step
			file_binary=True if row[3] == 1 else False
			# Store file data into XML
			doc_appinfo_files_file = doc.createElement("file")
			doc_appinfo_files.appendChild(doc_appinfo_files_file)
			doc_appinfo_files_file.setAttribute("id", "%s"%file_id)
			doc_appinfo_files_file.setAttribute("file_path",file_path)
			doc_appinfo_files_file.setAttribute("file_step", "%s"%file_step)
			doc_appinfo_files_file.setAttribute("file_binary", "%s"%file_binary)
		# Add application child to the XML
		doc_appinfo.appendChild(doc_appinfo_application)
		# Close connection
		db.close()
	except MySQLdb.Error, e:
		print 'exception!!!'
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to get application info")
		doc_error.setAttribute("sqlerr", repr(e))
		#doc_error.setAttribute("query", query)
		db.close()
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	return doc.toprettyxml(indent="\t")

# Following service returns the python code responsible of SGJP client execution
# this solution allows to manage only server-side code and do not deal with
# different client versions spread all aronund client side
@app.route("/sgjp")
def SGJP():
	print "<sgjp>"
	sys.stdout.flush()
	sgjp_client_file=open("SGJP_client.py","r")
	sgjp_client_file_text=sgjp_client_file.read()
	sgjp_client_file.close()
	return sgjp_client_file_text.replace('<SERVER_HOST>',sgjp_hostname).replace('<SERVER_PORT>',sgjp_port)

# Following service returns the python code responsible of SGJP notification service
# this solution allows to manage only server-side code and do not deal with
# different client versions spread all aronund client side
@app.route("/notifier")
def Notifier():
	print "<notifier>"
	sys.stdout.flush()
	sgjp_notifier_file=open("SGJP_notifier.py","r")
	sgjp_notifier_file_text=sgjp_notifier_file.read()
	sgjp_notifier_file.close()
	return sgjp_notifier_file_text

##----------------------------------------------------
## Follwong services are for client operations
##----------------------------------------------------

# userjobs <user_name> <time_interval_from> <time_interval_to>
# this service returns a list of tracked jobs for the specified time period
# in case no time interval is givem; place 'null' into the corresponding fields
# setting the user_name as null the service returns all tracked jobs
@app.route("/userjobs/<user_name>/<time_interval_from>/<time_interval_to>")
def userjobs(user_name,time_interval_from,time_interval_to):
	try:
		print "<userjobs> user_name: '%s' - time_interval_from: '%s' - time_interval_to: '%s'" % (user_name,time_interval_from,time_interval_to)
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Prepares the user_name clause
		user_clause=''
		if user_name.lower() != 'null':
			user_clause = " and job_uname='%s'" % user_name
		# Prepares the time_interval clause
		time_clause=''
		if time_interval_from.lower() != 'null':
			time_clause=time_clause+" and start_ts >= '%s'" % time_interval_from
		if time_interval_to.lower() != 'null':
			time_clause=time_clause+" and end_ts <= '%s'" % time_interval_to
		query="""select job_uname, job_tracking_id, start_ts, end_ts, job_desc from sgjp_tracked_jobs where 1=1"""
		query=query + user_clause + time_clause + ";" 
		#print "query: %s"%query
		cursor.execute(query)
		results = cursor.fetchall()
		doc_userjobs = doc.createElement("userjobs")
		doc.appendChild(doc_userjobs)
		for row in results:
			job_uname      =row[0]
			job_tracking_id=row[1]
			start_ts       =row[2]
			end_ts         =row[3]
			job_desc       =row[4]
			print "\t<tracking_record> job_tracking_id: %s - job_uname: '%s' - start_ts: %s - end_ts: %s - job_desc: '%s'" % (job_tracking_id,job_uname,start_ts,end_ts,job_desc)
			# Put output into the XML
			doc_userjobs_job = doc.createElement("job")
			doc_userjobs_job.setAttribute("job_tracking_id", "%s"%job_tracking_id)
			doc_userjobs_job.setAttribute("user_name", "%s"%job_uname)
			doc_userjobs_job.setAttribute("start_ts", "%s"%start_ts)
			doc_userjobs_job.setAttribute("end_ts", "%s"%end_ts)
			doc_userjobs_job.setAttribute("job_desc", "%s"%job_desc)
			doc_userjobs.appendChild(doc_userjobs_job)
		# Close connection
		db.close()
	except MySQLdb.Error, e:
		print 'exception!!!'
		db.close()
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to get user jobs")
		doc_error.setAttribute("sqlerr", repr(e))
		doc_error.setAttribute("query", query)
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	#return doc.toprettyxml(indent="\t")
	resp=Response(doc.toprettyxml(indent="\t"),status=200,mimetype='text/xml')
	return resp

# jobfiles <job_tracking_id>
# returns the list of files for a given job_tracking_id
@app.route("/jobfiles/<job_tracking_id>")
def jobfiles(job_tracking_id):
	try:
		print "<jobfiles> job_tracking_id: %s" % job_tracking_id
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Prepares the query
		query="""select file_id, file_path, file_step, file_binary  from sgjp_job_files where job_tracking_id=%s;""" % job_tracking_id
		print "query: %s"%query
		cursor.execute(query)
		results = cursor.fetchall()
		doc_jobfiles = doc.createElement("jobfiles")
		doc.appendChild(doc_jobfiles)
		for row in results:
			file_id    =row[0]
			file_path  =row[1]
			file_step  =row[2]
			file_binary=row[3]
			print "\t<tracked_file> job_tracking_id: %s - file_path: %s - file_step: %s - file_binary: '%s'" % (file_id,file_path,file_step,file_binary)
			# Put output into the XML
			doc_jobfiles_file = doc.createElement("file")
			doc_jobfiles_file.setAttribute("file_id", "%s"%file_id)
			doc_jobfiles_file.setAttribute("file_path", "%s"%file_path)
			doc_jobfiles_file.setAttribute("file_step", "%s"%file_step)
			doc_jobfiles_file.setAttribute("file_binary", "%s"%file_binary)
			doc_jobfiles.appendChild(doc_jobfiles_file)
		# Close connection
		db.close()
	except MySQLdb.Error, e:
		print 'exception!!!'
		db.close()
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to get job files")
		doc_error.setAttribute("sqlerr", repr(e))
		doc_error.setAttribute("query", query)
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	#return doc.toprettyxml(indent="\t")
	resp=Response(doc.toprettyxml(indent="\t"),status=200,mimetype='text/xml')
	return resp

# snapshots <job_tracking_id> <file_id>"
# snapshots returns a list of timestamps sorted in ascending order for a given 
# couple (job_tracking_id,file_id)
@app.route("/snapshots/<job_tracking_id>/<file_id>")
def snapshots(job_tracking_id,file_id):
	try:
		print "<snapshots> job_tracking_id: %s - file_id: %s" % (job_tracking_id,file_id)
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Prepares the query
		query="""select snapshot_ts from sgjp_snapshots where job_tracking_id=%s and file_id=%s order by 1 asc;""" % (job_tracking_id,file_id)
		print "query: %s"%query
		cursor.execute(query)
		results = cursor.fetchall()
		doc_snapshots = doc.createElement("snapshots")
		doc.appendChild(doc_snapshots)
		for row in results:
			snapshot_ts = row[0]
			print "\t<snapshot_record> job_tracking_id: %s - file_id: %s - snapshot_ts: %s" % (job_tracking_id,file_id,snapshot_ts)
			# Put output into the XML
			doc_snapshots_snapshot = doc.createElement("snapshot")
			doc_snapshots_snapshot.setAttribute("job_tracking_id", "%s"%job_tracking_id)
			doc_snapshots_snapshot.setAttribute("file_id", "%s"%file_id)
			doc_snapshots_snapshot.setAttribute("snapshot_ts", "%s"%snapshot_ts)
			doc_snapshots.appendChild(doc_snapshots_snapshot)
		# Close connection
		db.close()
	except MySQLdb.Error, e:
		print 'exception!!!'
		db.close()
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to get job files")
		doc_error.setAttribute("sqlerr", repr(e))
		doc_error.setAttribute("query", query)
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	#return doc.toprettyxml(indent="\t")
	resp=Response(doc.toprettyxml(indent="\t"),status=200,mimetype='text/xml')
	return resp


# getsnapshot <job_tracking_id> <file_id> <snapshot_ts>
# get_snapshot returns the content of a give snapshot (job_tracking_id,file_id,ts)
# if ts is null the whole snapshots will be returned 
@app.route("/getsnapshot/<job_tracking_id>/<file_id>/<snapshot_ts>")
def getsnapshot(job_tracking_id,file_id,snapshot_ts):
	try:
		print "<getsnapshot> job_tracking_id: %s - file_id: %s - snapshot_ts: '%s'" % (job_tracking_id,file_id,snapshot_ts)
		# Prepare the answer XML
		doc = Document()
		# Connect to the database
		db = MySQLdb.connect(sgjp_db_host,sgjp_db_user,sgjp_db_password,sgjp_db_name)
		cursor=db.cursor()
		# Prepares the time_interval clause
		snapshot_ts_clause=''
		if snapshot_ts.lower() != 'null':
			snapshot_ts_clause=snapshot_ts_clause+" and snapshot_ts = '%s'" % snapshot_ts
		query="""select snapshot_ts, text_content, binary_content from sgjp_snapshots where job_tracking_id=%s and file_id=%s""" % (job_tracking_id,file_id)
		query=query + snapshot_ts_clause + ";" 
		print "query: %s"%query
		cursor.execute(query)
		results = cursor.fetchall()
		doc_snapshots = doc.createElement("snapshots")
		doc.appendChild(doc_snapshots)
		for row in results:
			snapshot_ts    = row[0]
			text_content   = row[1]
			binary_content = row[2]
			print "<snapshot_record> snapshot_ts: %s - text_size: %s - binary_size: %s" % (snapshot_ts,sys.getsizeof(text_content),sys.getsizeof(binary_content))
			#print "--[snapshot (begin)]-------------------------------"
			#print text_content
			#print "--[snapshot (end)  ]-------------------------------"
			# Put output into the XML
			doc_snapshot = doc.createElement("snapshot")
			doc_snapshot.setAttribute("timestamp", "%s"%snapshot_ts)
			# Text
			if text_content != None:
				text_session = doc.createElement('text')
				text = doc.createTextNode(text_content)
				text_session.appendChild(text)
				doc_snapshot.appendChild(text_session)
			# Binary
			if binary_content != None:
				binary_session = doc.createElement('binary')
				binary = doc.createTextNode(base64.b64encode(binary_content))
				binary_session.appendChild(binary)
				doc_snapshot.appendChild(binary_session)
			doc_snapshots.appendChild(doc_snapshot)
		# Close connection
		db.close()
	except MySQLdb.Error, e:
		print 'exception!!!'
		db.close()
		doc_error = doc.createElement("error")
		doc.appendChild(doc_error)
		doc_error.setAttribute("message", "Unable to get job files")
		doc_error.setAttribute("sqlerr", repr(e))
		doc_error.setAttribute("query", query)
	sys.stdout.flush()
	#cherrypy.response.headers['Content-Type']= 'text/xml'
	#return doc.toprettyxml(indent="\t")
	resp=Response(doc.toprettyxml(indent="\t"),status=200,mimetype='text/xml')
	return resp



#
# Start the flask based SGJP server application
#
if __name__ == '__main__':
	config=Config()
	sgjp_hostname    = config.sgjp_hostname
	sgjp_port        = config.sgjp_port
	sgjp_db_host     = config.sgjp_db_host
	sgjp_db_user     = config.sgjp_db_user
	sgjp_db_password = config.sgjp_db_password
	sgjp_db_name     = config.sgjp_db_name
	app.debug = True
	app.run(host=sgjp_hostname,port=int(sgjp_port))

