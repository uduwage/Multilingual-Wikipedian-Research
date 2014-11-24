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

NOW = time.strftime("%Y_%m_%d_%H_%M")
OUT_DIR_LOGS = os.path.expanduser('~/logs')
OUT_DIR = os.path.expanduser('~/outputs/')
IS_GLOBAL_USER_FILES = os.path.expanduser('~/outputs/data_2014_04_18/')


'''
Create the logger
'''
def create_logger(logger, logLang='main'):
	log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	file_handler = logging.FileHandler(filename=(os.path.join(OUT_DIR_LOGS, logLang + '-babel_Users.log')))
	file_handler.setFormatter(log_format)
	logger.setLevel(logging.INFO)
	logger.addHandler(file_handler)


class CollectUserEditPatterns():

	def __init__(self):
		self.disconnect_database()
		self.con = None
		self.cur = None
		self.gdbCursor = None
		self.connectGlobalUserData()
		#self.filename = os.path.join(OUT_DIR + self.language + '-data.csv')
	

	def connect_database(self, lang):
		try:
			db_name = lang + 'wiki_p'
			self.con = mdb.connect(db=db_name, host=lang+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con))
			self.cur = self.con.cursor()
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
			self.gdbCursor.close()
			logging.info("Successfully disconnect " + self.language)
		except:
			pass

	def get_revision_time(self, user_id):
		"""
		user_id: Accept a user_id
		Return: True if user is active for past 4 weeks
		"""
		attempts = 0
		success = False
		while attempts < 3 and not success:
			revision_time_query = ur'''select rev_timestamp from revision_userindex where rev_user={uId} order by rev_timestamp desc limit 1'''.format(uId=int(user_id))
			try:
				self.cur.execute(revision_time_query)
				complete = False
				while not complete:
					last_rev_time = self.cur.fetchone()
					self.cur.fetchall()
					if not last_rev_time:
						complete = True
						continue
					convert_last_revtime = datetime.strptime(last_rev_time['rev_timestamp'], "%Y%m%d%H%M%S")
					if datetime.now() - convert_last_revtime < timedelta(weeks=4):
						return True
					else:
						return False
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)

	def get_active_days_from_first_edit(self, user_id, wiki):
		"""
		Get active days from the first edit count 6 months as inactive period
		"""
		revision_history_query = ur'''SELECT rev_id, rev_page, rev_user, rev_timestamp, rev_len FROM revision_userindex 
										WHERE rev_user= %s ORDER BY rev_timestamp ASC '''

		first_revision_query = ur'''SELECT rev_id, rev_page, rev_user, rev_timestamp, rev_len FROM revision_userindex 
										WHERE rev_user= %s ORDER BY rev_timestamp ASC LIMIT 1'''
		attempts = 0
		success = False
		while attempts < 3 and not success:
			try:
				self.cur.execute((first_revision_query % (user_id)).encode('utf-8'))
				complete = False
				convert_first_revtime = None
				while not complete:
					first_rev_time = self.cur.fetchone()
					self.cur.fetchall()
					if not first_rev_time:
						complete = True
						continue
					convert_first_revtime = datetime.strptime(first_rev_time['rev_timestamp'], "%Y%m%d%H%M%S")

				### Iterating over the rest of the history
				self.cur.execute((revision_history_query % (user_id)).encode('utf-8'))
				finish = False
				starting_point = convert_first_revtime # here starting point is the first revision date
				#import pdb
				#pdb.set_trace()
				edit_count = 0
				total_rev_length = 0
				lifespan = 0
				event = 0
				while not finish:
					rev_history_row = self.cur.fetchone()
					if not rev_history_row:
						finish = True
						continue
					current_record_revtime = datetime.strptime(rev_history_row['rev_timestamp'], "%Y%m%d%H%M%S")
					time_diff = current_record_revtime - starting_point
					if time_diff.days > timedelta(weeks=24).days:
						### if the gap is more than 6 months we use the starting_point which is the 
						### last revision prior to user's death to calculate the lifespan
						
						#--lifespan = (starting_point - convert_first_revtime).days
						#print user_id, current_record_revtime, time_diff.days, lifespan, edit_count, wiki
						#-- print str(user_id) + "," + str(lifespan) + "," + str(edit_count) + "," + str(total_rev_length) + "," + wiki
						event = 1
						self.cur.fetchall()
						break
					if rev_history_row['rev_len'] is not None:
						total_rev_length += int(rev_history_row['rev_len'])
					else:
						total_rev_length += 0
					edit_count += 1
					lifespan = (starting_point - convert_first_revtime).days
					starting_point = current_record_revtime # update the starting_point to the next revision
					if (datetime.now() - starting_point).days > timedelta(weeks=24).days:
						event = 1
				print str(user_id) + "," + str(lifespan) + "," + str(event) + "," + str(edit_count) + "," + str(total_rev_length) + "," + wiki
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)


	def is_user_global(self):
		completedfile = []
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			if filename not in completedfile:
				self.connect_database(filename.split("-")[0])
				completedfile.append(filename)
				file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
				input_file = csv.DictReader(open(file_location))
				completed_users = []
				match = 0
				unmatch = 0
				unmatch_active = 0
				for filerow in input_file:
					attempts = 0
					success = False
					if filerow['user_name'] not in completed_users:
						user_id = filerow['user_id']
						#import pdb
						#pdb.set_trace()
						while attempts < 3 and not success:
							uName = unicode(filerow['user_name'], 'utf-8')
							query = ur'''select gu_name from globaluser where gu_name = "%s"'''
							#query = ur'''select gu_name from globaluser where gu_name = "{uname}"'''.format(uname=unicode(filerow['user_name'],'utf-8', errors='strict'))
							
							try:
								#self.gdbCursor.execute(query.encode('utf-8'))
								self.gdbCursor.execute((query % (uName)).encode('utf-8'))
								gUser = self.gdbCursor.fetchone()
								if gUser:
									#print gUser['gu_name']
									match += 1
									self.gdbCursor.fetchall()
								else:
									unmatch += 1
									if self.get_revision_time(user_id):
										unmatch_active += 1
								success = True
							except Exception, e:
								attempts += 1
								traceback.print_exc()
								logging.exception(e)
						completed_users.append(filerow['user_name'])
				print filename.split("-")[0] + "," + str(match) + "," + str(unmatch) + "," + str(unmatch_active)

	def activity_lifespan(self):
		"""
		prepare for survival analysis: user_id, # of days active, failure/event, censored
		"""
		completedfile = []
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			if filename not in completedfile:
				#print filename
				self.disconnect_database()
				self.connect_database(filename.split("-")[0])
				completedfile.append(filename)
				file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
				input_file = csv.DictReader(open(file_location))
				completed_users = []
				for filerow in input_file:
					attempts = 0
					success = False
					if filerow['user_id'] not in completed_users:
						user_id = filerow['user_id']
						self.get_active_days_from_first_edit(user_id, filename.split("-")[0])
						completed_users.append(user_id)

	
									
def main():
	log = logging.getLogger()
	create_logger(log)
	#language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/ho_lang.csv"))
	#language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/wiki_lang_codes.csv"))
	collect = CollectUserEditPatterns()
	print "user_id, lifespan_days, total_edits, total_rev_length"
	collect.activity_lifespan()


if __name__=="__main__":
	main()

	"""
	def collect_user_edits(self):
		completedfile = []
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			#import pdb
			#pdb.set_trace()
			if filename not in completedfile:
				completedfile.append(filename)
				file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
				input_file = csv.reader(open(file_location, 'rb'))
				user_lang_dict = dict(x for x in input_file)
				if user_lang_dict:
					for userid, templates in user_lang_dict.iteritems():
						import pdb
						pdb.set_trace()
						for t in templates:
							print t
							#self.connect_database(filename.split('-')[0])
	"""				