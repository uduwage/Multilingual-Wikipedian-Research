import os
import sys, logging
import pywikibot
import csv
import MySQLdb as mdb
from MySQLdb import cursors
import traceback
import re
import time
from datetime import datetime, timedelta
import argparse
import pdb

'''
Create the logger
'''
NOW = time.strftime("%Y_%m_%d_%H_%M")
OUT_DIR_LOGS = os.path.expanduser('~/logs')

def create_logger(logger, logLang='main'):
	log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	file_handler = logging.FileHandler(filename=(os.path.join(OUT_DIR_LOGS, logLang + '-babel_Users.log')))
	file_handler.setFormatter(log_format)
	logger.setLevel(logging.INFO)
	logger.addHandler(file_handler)

class EditPatternUserProficency():

	def __init__(self):
		self.dir  = os.path.expanduser('~/outputs/data_10_10_bigwiki/')
		self.outdir = os.path.expanduser('~/outputs/')
		self.disconnect_database()
		self.con = None
		self.cur = None
		self.con2 = None
		self.cur2 = None
		self.gdbCursor = None
		self.connectGlobalUserData()

	def disconnect_database(self):
		try:
			self.cur.close()
			self.con.close()
			self.gdbCursor.close()
			logging.info("Successfully disconnect " + self.language)
		except:
			pass

	def connect_database(self, dbLang):
		try:
			db_name = dbLang+ 'wiki_p'
			self.con = mdb.connect(db=db_name, host=dbLang+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con))
			self.cur = self.con.cursor()
		except mdb.Error, e:
			logging.error("Unable to establish connection")
		try:
			db_name2 = dbLang + 'wiki_p'
			self.con2 = mdb.connect(db=db_name2, host=dbLang+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
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

	def _isUserGlobal(self, user_name):
		logging.info("checking if " + user_name + " is  global")
		attempts = 0
		success = False
		while attempts < 3 and not success:
			unicode_user_name = unicode(user_name, 'utf-8')
			query = ur'''select gu_name from globaluser where gu_name = "%s"'''
			try:
				#self.gdbCursor.execute(query.encode('utf-8'))
				self.gdbCursor.execute((query % (unicode_user_name)).encode('utf-8'))
				gUser = self.gdbCursor.fetchone()
				if gUser:
					#print gUser['gu_name']
					self.gdbCursor.fetchall()
					return True
				else:
					self.gdbCursor.fetchall()
					return False
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)						

	def user_namespace_contribution(self):

		lang_code = re.compile(ur'(?P<lcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		
		query_groupby_namespace = ur'''SELECT count(rev_id) AS rev_count, sum(rev_len) AS total_rev_length, page_namespace 
									FROM revision_userindex, page 
									WHERE page_id = rev_page 
									AND rev_user = %(user_id)s GROUP BY page_namespace'''
		
		with open(os.path.expanduser(self.outdir + 'ms_proficency_namespace_with_none_global.csv'), 'w+') as data_file:
			writer = csv.writer(data_file)
			writer.writerow(('user_id', 'rev_count', 'total_rev_length', 'page_namespace', 'proficency_level'))
			for filename in os.listdir(self.dir):
				completed_users = []
				wiki_code = filename.split("-")[0]
				self.connect_database(filename.split("-")[0])
				input_file = csv.DictReader(open(self.dir + filename))
				for filerow in input_file:
					if filerow['user_id'] not in completed_users:						
						match_template = lang_code.match(filerow['template'])
						if match_template:
							a = [wiki_code+'-1', wiki_code+'-2', wiki_code+'-3', wiki_code+'-4', wiki_code+'-5', wiki_code+'-N', wiki_code+'-n', wiki_code+'-M']
							if any(x in filerow['template'] for x in a):
								#if self._isUserGlobal(filerow['user_id']):
								attempt = 0
								success = False
								while attempt < 3 and not success:
									try:
										self.cur.execute(query_groupby_namespace, {'user_id':filerow['user_id']})
										logging.info("processing user " + filerow['user_id'])
										complete = False
										while not complete:
											group_by_namespace_data = self.cur.fetchone()
											#self.cur.fetchall()
											if not group_by_namespace_data:
												complete = True
												continue
											writer.writerow([filerow['user_id'], group_by_namespace_data['rev_count'], 
												group_by_namespace_data['total_rev_length'], group_by_namespace_data['page_namespace'], filerow['template']])
											completed_users.append(filerow['user_id'])
										success = True
									except Exception, e:
										attempt += 1
										traceback.print_exc()
										logging.exception(e)
									except mdb.OperationalError, sqlEx:
										attempt += 1
										if sqlEx[0] == 2006:
											logging.info("Caught the MySQL server gone away exception attempting to re-connect")
											logging.error(sqlEx)
											self.connect_database(filename.split("-")[0])
								#else:
								#	logging.info(filerow['user_id'] + " is not a global user in " + wiki_code)


	def edit_count_by_proficency(self):
		lang_code = re.compile(ur'(?P<lcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		
		query_edit_count = ur'''SELECT user_editcount FROM user WHERE user_id = %(user_id)s'''
		completed_users = []
		with open(os.path.expanduser(self.outdir + 'ms_proficency_editcount.csv'), 'w+') as data_file:
			writer = csv.writer(data_file)
			writer.writerow(('user_id', 'edit_count', 'proficency_level'))
			for filename in os.listdir(self.dir):
				self.connect_database(filename.split("-")[0])
				input_file = csv.DictReader(open(self.dir + filename))
				for filerow in input_file:
					logging.info("processing user_id " + filerow['user_id'])
					match_template = lang_code.match(filerow['template'])
					#print filerow
					if match_template:
						a = ['ms-1', 'ms-2', 'ms-3', 'ms-4', 'ms-5', 'ms-N', 'ms-n', 'ms-M']
						if any(x in filerow['template'] for x in a):
							attempt = 0
							success = False
							while attempt < 3 and not success:
								try:
									self.cur.execute(query_edit_count, {'user_id':filerow['user_id']})
									complete = False
									while not complete:
										user_edit_count = self.cur.fetchone()
										#self.cur.fetchall()
										if not user_edit_count:
											complete = True
											continue
										#print user_edit_count
										writer.writerow([filerow['user_id'], user_edit_count['user_editcount'], filerow['template']])
									success = True
								except Exception, e:
									attempt += 1
									traceback.print_exc()
									logging.exception(e)
								except mdb.OperationalError, sqlEx:
									attempt += 1
									if sqlEx[0] == 2006:
										logging.info("Caught the MySQL server gone away exception attempting to re-connect")
										logging.error(sqlEx)
										self.connect_database(filename.split("-")[0])

	def _get_rev_count(self, user_id):
		query_distinct_title_count = ur'''SELECT COUNT(distinct(rev_page)) as REV_COUNT 
										FROM revision_userindex, page  
										WHERE page_id = rev_page 
										AND page_namespace=0 
										AND rev_user = %(user_id)s'''
		attempt = 0
		success = False
		while attempt < 3 and not success:
			try:
				self.cur2.execute(query_distinct_title_count, {'user_id':user_id})
				complete = False
				while not complete:
					rev_count = self.cur2.fetchone()
					self.cur2.fetchall()
					if not rev_count:
						complete = True
						continue
					user_rev_count = rev_count['REV_COUNT']
					print user_rev_count
				success = True
				return user_rev_count
			except Exception, e:
				attempt += 1
				traceback.print_exc()
				logging.exception(e)										


	def get_titles(self):
		lang_code = re.compile(ur'(?P<lcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)


		query_page_title = ur'''SELECT distinct(p.page_title) 
								FROM revision_userindex r, page p  
								WHERE p.page_id = r.rev_page 
								AND p.page_namespace=0 
								AND r.rev_user = %(user_id)s ORDER by RAND() LIMIT 100'''
		with open(os.path.expanduser(self.outdir + 'en1_titles.csv'), 'w+') as en1_file, open(os.path.expanduser(self.outdir + 'enN_titles.csv'), 'w+') as enN_file:
			en1writer = csv.writer(en1_file)
			enNwriter = csv.writer(enN_file)
			for filename in os.listdir(self.dir):
				self.connect_database(filename.split("-")[0])
				input_file = csv.DictReader(open(self.dir + filename))
				wiki_code = filename.split("-")[0]
				for filerow in input_file:
					template_from_row = filerow['template']
					logging.info("processing user_id " + filerow['user_id'])
					match_template = lang_code.match(filerow['template'])
					if match_template:
						a = [wiki_code+'-1', wiki_code+'-2', wiki_code+'-3', wiki_code+'-4', wiki_code+'-5', wiki_code+'-N', wiki_code+'-n', wiki_code+'-M']
						if any(code in filerow['template'] for code in a):
							rev_count = self._get_rev_count(filerow['user_id'])
							if wiki_code+'-N' == template_from_row:
								if int(rev_count) > 100:
									attempt = 0
									success = False
									while attempt < 3 and not success:
										try:
											self.cur.execute(query_page_title, {'user_id':int(filerow['user_id'])})
											titles = self.cur.fetchall()
											for title in titles:
												enNwriter.writerow([title['page_title'].replace("_", " ")])
											success = True
										except Exception, e:
											attempt += 1
											traceback.print_exc()
											logging.exception(e)
										except mdb.OperationalError, sqlEx:
											attempt += 1
											if sqlEx[0] == 2006:
												logging.info("Caught the MySQL server gone away exception attempting to re-connect")
												logging.error(sqlEx)
												self.connect_database(filename.split("-")[0])
							elif wiki_code+'-1' == template_from_row:
								attempt = 0
								success = False
								while attempt < 3 and not success:									
									try:
										self.cur.execute(query_page_title, {'user_id':int(filerow['user_id'])})
										titles = self.cur.fetchall()
										for title in titles:
											en1writer.writerow([title['page_title'].replace("_", " ")])
										success = True
									except Exception, e:
										attempt += 1
										traceback.print_exc()
										logging.exception(e)
									except mdb.OperationalError, sqlEx:
										attempt += 1
										if sqlEx[0] == 2006:
											logging.info("Caught the MySQL server gone away exception attempting to re-connect")
											logging.error(sqlEx)
											self.connect_database(filename.split("-")[0])
							
def main():
	log = logging.getLogger()
	create_logger(log)
	editPattern = EditPatternUserProficency()
	#editPattern.edit_count_by_proficency()
	#editPattern.user_namespace_contribution()
	editPattern.get_titles()

if __name__ == "__main__":
	main()
