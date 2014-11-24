import os
import sys, logging
import pywikibot
import csv
import MySQLdb as mdb
from MySQLdb import cursors
import traceback
import re
import time

NOW = time.strftime("%Y_%m_%d_%H_%M")
OUT_DIR_LOGS = os.path.expanduser('~/logs')
OUT_DIR = os.path.expanduser('~/outputs/')
IS_GLOBAL_USER_FILES = os.path.expanduser('~/outputs/data_2014_03_27/')

'''
Reads the language code file and return a list of language codes
'''
def getLanguageCodes(file_name):
	"""
	Read language code from csv file and return a list of laguage codes
	This is the 286 languages in current wikipedia.
	"""
	try:
		lang_code_file = open(file_name, 'r')
		lang_code = lang_code_file.read().splitlines()
		lang_code_file.close()
		reversed_lang_list = list(reversed(lang_code))
		return reversed_lang_list
	except IOError as e:
		logging.error('unable to open language code file:')
		logging.error(str(e))

'''
Reads the pipe seperated dialect codes from the file and return as a list.
'''
def getDialectCodes(file_name):
	"""
	Read iso code file that with double pipe delimiter replace the the double pipe
	with single and returns the generator. Generator gets thrown into the csvreader
	which returns iterable object (list)
	"""
	dialect_codes = set()
	d_pipe_file_handle = open(file_name, 'r')
	csv_reader = csv.reader((line.replace('||', '|') for line in d_pipe_file_handle), delimiter='|')
	for row in csv_reader:
		dialect_codes.add(row[0])
	return dialect_codes

'''
Create the logger
'''
def create_logger(logger, logLang='main'):
	log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	file_handler = logging.FileHandler(filename=(os.path.join(OUT_DIR_LOGS, logLang + '-babel_Users.log')))
	file_handler.setFormatter(log_format)
	logger.setLevel(logging.INFO)
	logger.addHandler(file_handler)


class GlobalUserIntersect():

	def __init__(self, language, limit):
		self.disconnect_database()
		self.con = None
		self.cur = None
		self.con2 = None
		self.cur2 = None
		self.gdbCursor = None
		self.language = language
		self.connect_database()
		self.connectGlobalUserData()
		self.limit = limit
		self.filename = os.path.join(OUT_DIR + self.language + '-data.csv')
	

	def connect_database(self):
		try:
			db_name = self.language + 'wiki_p'
			self.con = mdb.connect(db=db_name, host=self.language+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con))
			self.cur = self.con.cursor()
		except mdb.Error, e:
			logging.error("Unable to establish connection")
		try:
			db_name2 = self.language + 'wiki_p'
			self.con2 = mdb.connect(db=db_name2, host=self.language+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con2))
			self.cur2 = self.con2.cursor()
		except mdb.Error, e:
			logging.error("Unable to establish connection")

	def connectGlobalUserData(self):
		"""
		Connect to global accounts. This is a different database than connectServer.
		We are not using this method at the moment.
		"""
		try:
			logging.info("Connecting to Toolserver MySql Db: mysql -h sql-s3 centralauth_p")  # "mysql -hcentralauth-p.userdb"
			print "Connecting to Toolserver MySql Db: mysql -h sql-s3 centralauth_p"
			self.gdbConnection = mdb.connect(db='centralauth_p',
												 host="centralauth.labsdb",
												 read_default_file=os.path.expanduser("~/replica.my.cnf"))
			logging.info("Connection Successful" + str(self.gdbConnection))
			self.gdbCursor = self.gdbConnection.cursor(cursors.SSDictCursor)
		except mdb.Error, e:
			logging.error("Unable to establish connection")
		

	def disconnect_database(self):
		try:
			self.cur.close()
			self.con.close()
			self.cur2.close()
			self.con2.close()
			self.gdbCursor.close()
			logging.info("Successfully disconnect " + self.language)
		except:
			pass

	def is_user_global(self):
		completedfile = []
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			if filename not in completedfile:
				file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
				input_file = csv.DictReader(open(file_location))
				completed_users = []
				match = 0
				unmatch = 0
				for filerow in input_file:
					attempts = 0
					success = False
					if filerow['user_name'] not in completed_users:
						while attempts < 3 and not success:
							query = ur'''select gu_name from globaluser where gu_name = "{uname}"'''.format(uname=unicode(filerow['user_name'],'utf-8', errors='strict'))
							try:
								self.gdbCursor.execute(query.encode('utf-8'))
								gUser = self.gdbCursor.fetchone()
								if gUser:
									#print gUser['gu_name']
									match += 1
								else:
									unmatch += 1
								self.gdbCursor.fetchall()
								success = True
							except Exception, e:
								attempts += 1
								traceback.print_exc()
								logging.exception(e)
						completed_users.append(filerow['user_name'])
				print filename.split("-")[0], match, unmatch
				completedfile.append(filename)				

def main():
	log = logging.getLogger()
	create_logger(log)
	#language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/ho_lang.csv"))
	language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/google_doc_langs.csv"))

	for language_code in language_list:
		globalusers = GlobalUserIntersect(language_code, '')
		globalusers.is_user_global()

if __name__=="__main__":
	main()		