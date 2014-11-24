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
OUT_DIR = os.path.expanduser('~/outputs/data_lifetime_edits_05_08/')
IS_GLOBAL_USER_FILES = os.path.expanduser('~/outputs/data_big_wiki/')
OUT_PUT_LESS_TEN_THOU = os.path.expanduser('~/outputs/less_10000/')

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


class CollectUserEditPatterns():

	def __init__(self):
		self.disconnect_database()
		self.disconnect_globaldb()
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
		
	def disconnect_globaldb(self):
		try:
			self.gdbCursor.close()
			self.gdbConnection.close()
		except:
			pass

	def disconnect_database(self):
		try:
			self.cur.close()
			self.con.close()
			logging.info("Successfully disconnect " + self.language)
		except:
			pass

	def is_user_active(self, wiki, user_id):
		'''
		Return True if gap between user's last revision and now is less than 4 weeks
		Return False else:
		'''
		self.connect_database(wiki)
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
					if datetime.now() - convert_last_revtime <= timedelta(weeks=4):
						self.disconnect_database()
						return True
					else:
						self.disconnect_database()
						return False
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)


#+        current_week_start_date = start_date + timedelta(days=(7* week))
#+        current_week_end_date = start_date + timedelta(days=(7 * (week + 1)))

	def get_totaledits_lifespan(self, wiki, user_id):
		"""
		Method currently generate data to understand the life time between registration and last_edit
		Only for active users who has global accounts
		Active: Time difference between last revision and now is <= 4 weeks
		"""
		print user_id
		self.connect_database(wiki)

		# Query - registered day of the user for the given wiki
		registered_day_query = ur'''SELECT user_registration FROM user WHERE user_id = {uId}'''.format(uId=int(user_id))
		
		# Query - get last revision day
		last_revision_query = ur'''select rev_timestamp from revision_userindex where rev_user={uId} order by rev_timestamp desc limit 1'''.format(uId=int(user_id))

		# Query - get total edit count per user
		edit_count_query = ur'''SELECT user_editcount FROM user WHERE user_id = {userId}'''.format(userId=int(user_id))

		# Query - get weekly edit history
		weekly_edit_history_query = ur'''SELECT rev_len FROM revision_userindex WHERE rev_timestamp >= {start_time_stamp} AND rev_timestamp <= {end_time_stamp}'''
		
		user_data = None
		attempts = 0
		success = False
		while attempts < 3 and not success:
			#import pdb
			#pdb.set_trace()
			convert_registered_day = None
			converted_last_rev_day = None
			try:
				self.cur.execute(registered_day_query)
				complete = False
				while not complete:
					registered_day = self.cur.fetchone()
					self.cur.fetchall()
					if not registered_day:
						complete = True
						continue
					if registered_day['user_registration']:
						convert_registered_day = datetime.strptime(registered_day['user_registration'], "%Y%m%d%H%M%S")
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)				
			try:
				# Getting total edits for the user
				self.cur.execute(edit_count_query)
				finish = False
				while not finish:
					edit_count = self.cur.fetchone()
					self.cur.fetchall()
					if not edit_count:
						finish = True
						continue
					user_edit_count = edit_count['user_editcount']
				# Get the revision data to calculate the delta between registration and last edit.
				self.cur.execute(last_revision_query)
				complete = False
				while not complete:
					last_rev_day = self.cur.fetchone()
					self.cur.fetchall()
					if not last_rev_day:
						complete = True
						continue
					converted_last_rev_day = datetime.strptime(last_rev_day['rev_timestamp'], "%Y%m%d%H%M%S")	
				# calculate time delta
				if converted_last_rev_day is not None and convert_registered_day is not None:
					date_delta = (converted_last_rev_day - convert_registered_day).days
					print wiki, user_id, date_delta, user_edit_count
					user_data = (user_id, date_delta, user_edit_count, wiki)
					return user_data
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)				

	def is_user_global(self, user_name):
		"""
		Given a user check if the user has a global account or not
		return: User's global wiki name
		return: False if not
		"""
		attempts = 0
		success = False

		while attempts < 3 and not success:
			#uName = unicode(user_name,'utf-8', errors='strict')
			global_user_name = unicode(user_name, 'utf-8')
			query = ur'''SELECT gu_name, gu_home_db FROM globaluser WHERE gu_name = %(uName)s'''
			try:
				self.gdbCursor.execute(query, {'uName':global_user_name.encode('utf-8')})
				gUser = self.gdbCursor.fetchone()
				if gUser is not None:
					self.gdbCursor.fetchall()
					return gUser['gu_home_db']
				else:
					self.gdbCursor.fetchall()
					return False
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)

	def collect_user_edits(self):
		"""
		Collect edit history for the users
		"""
		completedfile = []
		### Get all the files with template data
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			output_file_name = os.path.join(OUT_DIR + filename.split("-")[0] + '-lifespan-edit.csv')
			data_file = open(output_file_name, "wt")
			with open(os.path.expanduser(output_file_name), 'w+') as data_file:
				if filename not in completedfile:
					completedfile.append(filename)
					file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
					input_file = csv.DictReader(open(file_location))
					user_template_dict = {} # user_id and their template dictionary
					completed_userids = [] # this is to keep track of the users checked for global.
					user_global = None
					for row in input_file:
						if row['user_id'] not in completed_userids:
							### checking of the user is global
							user_global = self.is_user_global(row['user_name'])
							completed_userids.append(row['user_id'])
						### if user is global we collect the user for further analysis
						if user_global and self.is_user_active(filename.split("-")[0], row['user_id']):	
							if row['user_id'] not in user_template_dict:
								user_template_dict[row['user_id']] = []
								user_template_dict[row['user_id']].append(row["template"])
							else:
								user_template_dict[row["user_id"]].append(row["template"])
					# getting edit history
					for key, value in user_template_dict.iteritems():
						x = self.get_totaledits_lifespan(filename.split("-")[0], key)
						writer = csv.writer(data_file)
						if x is not None:
							writer.writerow((x[0], x[1], x[2], x[3]))
						else:
							logging.info(key, filename)

	def users_inLargeWiki_contribute_to_small(self):
		small_wiki_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/small_wikis.csv"))
		completedfile = []
		for filename in os.listdir(IS_GLOBAL_USER_FILES):
			#data_file = open(output_file_name, "wt")
			print filename
			if filename not in completedfile:
				completedfile.append(filename)
				file_location = os.path.expanduser(IS_GLOBAL_USER_FILES + filename)
				input_file = csv.DictReader(open(file_location))
				output_file_path = os.path.join(OUT_PUT_LESS_TEN_THOU + 'users-from-' + filename.split('-')[0] + '-data.csv')
				with open(os.path.expanduser(output_file_path), 'w+') as big_to_small_contrib:
					writer = csv.writer(big_to_small_contrib)
					completed_userids = []
					user_global = None
					for row in input_file:
						if row['user_id'] not in completed_userids:
							# checking if user is global user - to avoid multiple db calls
							user_global = self.is_user_global(row['user_name']) # Returns the user's home wiki
						# If user is global then we start processing the user
						if user_global:
							usr_template = row["template"]
							name = unicode(row['user_name'], 'utf-8')
							wiki = None
							self.connect_database(user_global.split('wiki')[0])
							# prepare query for home wiki edit
							home_edit_query = ur'''SELECT user_editcount FROM user WHERE user_name = %(usr_name)s'''
							home_edit_count = None
							s = False
							a = 0
							while a < 3 and not s:
								try:
									self.cur.execute(home_edit_query, {'usr_name':name.encode('utf-8')})
									x = self.cur.fetchone()
									if x is not None:
										home_edit_count = x['user_editcount']
										self.cur.fetchall()
									else:
										self.cur.fetchall()
									s = True
								except Exception, e:
									a += 1
									traceback.print_exc()
									logging.exception(e)
							# confirm the template is in the articles less than 10000 list											
							if usr_template.split("_")[1] in small_wiki_list or usr_template.split("_")[1].split("-")[0] in small_wiki_list:
								# Since our csv files have version User_en-3 and User_en control structure is been used to get clear name
								if len(usr_template.split("_")[1].split("-")) > 1:
									wiki = usr_template.split("_")[1].split("-")[0]
									self.connect_database(wiki)
								else:
									wiki = usr_template.split("_")[1]
									self.connect_database(wiki)
								# query to get the edits of the user mentioned language
								edit_query = ur'''SELECT user_editcount FROM user WHERE user_name = %(uname)s'''
								edit_count = None
								if (row['user_id'], wiki) not in completed_userids:
									success = False
									attempts = 0
									while attempts < 3 and not success:
										try:
											self.cur.execute(edit_query, {'uname':name.encode('utf-8')})
											x = self.cur.fetchone()
											if x is not None:
												edit_count = x['user_editcount']
												self.cur.fetchall()
											else:
												edit_count = 0
												self.cur.fetchall()
											success = True
										except Exception, e:
											attempts += 1
											traceback.print_exc()
											logging.exception(e)
									writer.writerow((row['user_id'], row['user_name'], user_global, str(home_edit_count), usr_template.split("_")[1], str(edit_count)))
									completed_userids.append((row['user_id'], wiki))
								else:
									logging.info("we have seen the user id in the wiki")
							else:
								logging.info("template is not in the small wiki list")


def main():
	log = logging.getLogger()
	create_logger(log)
	#language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/ho_lang.csv"))
	language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/wiki_lang_codes_latest.csv"))
	collect = CollectUserEditPatterns()
	#collect.collect_user_edits()
	collect.users_inLargeWiki_contribute_to_small()


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