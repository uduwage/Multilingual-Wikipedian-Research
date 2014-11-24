import os
import sys, logging
import pywikibot
import csv
import MySQLdb as mdb
from MySQLdb import cursors
import traceback
import re
import time
import argparse
import pandas

def disconnect_db(cur, con):
	try:
		cur.close()
		con.close()
		logging.info("Successfully disconnect")
	except:
		pass

def connect_database(dbLang):
	try:
		formatted_dbLang = dbLang.replace('-','_')
		db_name = formatted_dbLang + 'wiki_p'
		con = mdb.connect(db=db_name, host=dbLang+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
		logging.info("Connection Successful: " + str(con))
		cur = con.cursor()
		return cur
	except mdb.Error, e:
		logging.error("Unable to establish connection")

def connectGlobalUserData():
	"""
	Connect to global accounts. This is a different database than connectServer.
	We are not using this method at the moment.
	"""
	try:
		logging.info("Connecting to Toolserver MySql Db: mysql -h sql-s3 centralauth_p")  # "mysql -hcentralauth-p.userdb"
		print "Connecting to Toolserver MySql Db: mysql -h sql-s3 centralauth_p"
		gdbConnection = mdb.connect(db='centralauth_p',
											 host="centralauth.labsdb",
											 read_default_file=os.path.expanduser("~/replica.my.cnf"))
		logging.info("Connection Successful" + str(gdbConnection))
		gdbCursor = gdbConnection.cursor(cursors.SSDictCursor)
		return gdbCursor
	except mdb.Error, e:
		logging.error("Unable to establish connection")

def get_last_row(csv_file):
	with open(csv_file, 'rb') as f:
		reader = csv.reader(f)
		lastLine = reader.next()
		for line in reader:
			lastLine = line
		return lastLine

