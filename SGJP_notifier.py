#!/usr/bin/env python
#
# SGJP_notifier.py - Science Gateway Job Perusal notifier service 
#
# This script sends a preformatted and HTML encoded email containing images 
# to a destination mail address
#
# Riccardo Bruno (riccardo.bruno@ct.infn.it)
#
import os,sys,base64
import urllib, urllib2
from xml.dom.minidom import parseString

#
# Class that manages the 
#
class SGJP_Notifier:
	def __init__(self\
	            ,sgjp_host       \
	            ,sgjp_port       \
	            ,mail_from       \
	            ,mail_to         \
	            ,mail_subj       \
	            ,mail_body       \
	            ,mail_images_list\
	            ):
		# Assing class values
		self.sgjp_host        = sgjp_host
		self.sgjp_port        = sgjp_port
		self.mail_from        = mail_from
		self.mail_to          = mail_to
		self.mail_subj        = mail_subj
		self.mail_body        = mail_body
		self.mail_images_list = mail_images_list
		# Send POST request

	def send(self):
		try:
			# Load email body text
			file_mail_body = open(self.mail_body, "r")
			self.mail_body=file_mail_body.read()
			file_mail_body.close()
			# Load email image logo
			file_mail_logo = open(self.mail_images_list[0],"rb")
			self.mail_logo=file_mail_logo.read()
			encoded_mail_logo = base64.b64encode(self.mail_logo)
			#print "---[encoded_mail_logo]---------------------"
			#print encoded_mail_logo
			#print "-------------------------------------------"
			file_mail_logo.close()
			rest_query='http://%s:%s/notify' % (self.sgjp_host,self.sgjp_port)
			#print 'notify rest query: %s' % rest_query
			values = [ ('email_from', '%s' % self.mail_from)
			          ,('email_to'  , '%s' % self.mail_to  )
			          ,('email_subj', '%s' % self.mail_subj)
			          ,('email_body', '%s' % self.mail_body)
			          ,('email_logo', '%s' % encoded_mail_logo)
			          ,('type'      , 'base64'             ) ]
			data = urllib.urlencode(values)
			req = urllib2.Request(rest_query)
			data = urllib2.urlopen(rest_query,data).read()
			dom = parseString(data)
			xml_error = dom.getElementsByTagName('error')
			if(xml_error!=[]):
				err_message = xml_error[0].getAttribute("message")
				print "ERROR : '%s'"%err_message
				return 30
			else:
				xml_track = dom.getElementsByTagName('result')
				xml_track_tag  = xml_track[0]
				result = xml_track_tag.getAttribute("status")
		except urllib2.HTTPError, e:
			print "HTTP error: %d" % e.code
			return 20
		except urllib2.URLError, e:
			print "Network error: %s" % e.reason.args[1]
			return 10
		return 0

#
# Start SGJP_notifier application
#
if __name__ == '__main__':
	# Get inputs
	if len(sys.argv) < 8:
		print """Usage: %s <SGJP_hostname> <SGJP_port> <mail_from> <mail_to> <mail_subj> <mail_body> <mail_image1> [<mail_image> ...]""" % os.path.basename(sys.argv[0])
		sys.exit(1)
		
	sgjp_hostname = sys.argv[1]
	sgjp_port     = sys.argv[2]
	mail_from     = sys.argv[3]
	mail_to       = sys.argv[4]
	mail_subj     = sys.argv[5]
	mail_body     = sys.argv[6]
	mail_images   = ()
	for mail_image in sys.argv[7:]:
		mail_images+=(mail_image,)
	# Create the Notifier object
	notifier = SGJP_Notifier(sgjp_hostname\
	                        ,sgjp_port    \
	                        ,mail_from    \
	                        ,mail_to      \
	                        ,mail_subj    \
	                        ,mail_body    \
	                        ,mail_images
	                        )
	# Send the mail
	sys.exit(notifier.send())

