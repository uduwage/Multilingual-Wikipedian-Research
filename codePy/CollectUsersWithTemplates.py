#!/usr/bin/python
# -*- coding: utf-8  -*-
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

NOW = time.strftime("%Y_%m_%d_%H_%M")
OUT_DIR_LOGS = os.path.expanduser('~/logs')
OUT_DIR = os.path.expanduser('~/outputs/All_Template_Users/data_11_24_2014/')

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
def create_logger(logger, logLang):
	log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	file_handler = logging.FileHandler(filename=(os.path.join(OUT_DIR_LOGS, logLang + '-babel_Users.log')))
	file_handler.setFormatter(log_format)
	logger.setLevel(logging.INFO)
	logger.addHandler(file_handler)


class CollectUsersWithTemplate():

	def __init__(self, language, limit):
		self.disconnect_database()
		self.con = None
		self.cur = None
		self.con2 = None
		self.cur2 = None
		self.language = language
		self.connect_database()
		self.limit = limit
		if self.language == 'ho' or self.language == 'aa' or self.language == 'cho' or self.language == 'ng':
			self.translatedString = ur'''User'''
		elif self.language =='it':
			self.translatedString = ur'''Utenti'''
		elif self.language == 'es':
			self.translatedString = ur'''Wikipedia:Wikipedistas\_con\_%'''
		elif self.language == 'ast':
			self.translatedString = ur'''Usuarios\_por\_idioma\_%'''
		elif self.language == 'ay':
			self.translatedString = ur'''Aru%'''
		elif self.language == 'ang':
			self.translatedString = u"Brūcend"
		elif self.language == 'ca':
			self.translatedString = u"Usuaris"
		else:
			self.translatedString = self.getTranslatedStringForUser()
		self.filename = os.path.join(OUT_DIR + self.language + '_data.csv')


	'''
	Make an api call to get the localized version of the word "user" for given language.
	'''
	def getTranslatedStringForUser(self):
		"""
		Gets the local namespace name for User pages. e.g. Bruker on no.

		Uses pywikibot.

		API method:
			https://no.wikipedia.org/w/api.php?action=query&meta=siteinfo
				 &siprop=namespaces&format=json
		"""
		try:
			logging.info("Fetching User Namespace Name")
			format_language = self.language
			if '_' in format_language:
				wikiSite = pywikibot.getSite(format_language.replace('_','-'))
			else:
				wikiSite = pywikibot.getSite(self.language)
			#print wikiSite
			r = pywikibot.data.api.Request(
				site=wikiSite, action="query", meta="siteinfo")
			r['siprop'] = u'namespaces'
			data = r.submit()
			if self.language == 'pt':
				localized_user = data['query']['namespaces']['2']['*']
				return localized_user.split('(')[0]
			else:
				return data['query']['namespaces']['2']['*']
		except pywikibot.exceptions.NoSuchSite, e:
			logging.error(e)		

	def connect_database(self):
		try:
			format_lang_string = self.language.replace('-','_')
			db_name = format_lang_string+ 'wiki_p'
			self.con = mdb.connect(db=db_name, host=format_lang_string+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con))
			self.cur = self.con.cursor()
		except mdb.Error, e:
			logging.error("Unable to establish connection")
		try:
			format_lang_string = self.language.replace('-','_')
			db_name2 = format_lang_string + 'wiki_p'
			self.con2 = mdb.connect(db=db_name2, host=format_lang_string+"wiki.labsdb", read_default_file=os.path.expanduser("~/replica.my.cnf"),  cursorclass=mdb.cursors.SSDictCursor)
			logging.info("Connection Successful: " + str(self.con2))
			self.cur2 = self.con2.cursor()
		except mdb.Error, e:
			logging.error("Unable to establish connection")

	def disconnect_database(self):
		try:
			self.cur.close()
			self.con.close()
			self.cur2.close()
			self.con2.close()
			logging.info("Successfully disconnect " + self.language)
		except:
			pass

	'''
	Given a specific language get all the flag bots and return as a list.
	At some point we are going to need another method to detect bots that are not flagged. 
	'''
	def get_bot_users(self):
		logging.info("Getting bot users")
		bots = list()
		bot_query = ur'''SELECT /* SLOW_OK LIMIT: 1800 */ user_id from user JOIN user_groups ON user_id = ug_user where ug_group = 'bot' '''
		success = False
		attempts = 0
		while attempts < 3 and not success:
			try:
				self.cur.execute(bot_query)
				complete = False
				while not complete:
					row = self.cur.fetchone()
					if not row:
						complete = True
						continue
				#for row in self.cur:
					bots.append(row['user_id'])
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)
		return bots

	def get_spanish_lang_categories(self):
		'''
		Spanish wiki has non conventional cats so we first capture them to match it up later.
		'''
		logging.info("Getting the spanish categories")
		cat_query = ur'''SELECT /* SLOW_OK LIMIT: 1800 */ cl_to
							FROM page
							INNER JOIN categorylinks
							ON (page_id=cl_from)
							WHERE cl_to LIKE "Wikipedia:Wikipedistas_con_%"
							AND page_namespace=2 group by cl_to'''
		cats = list()
		success = False
		attempts = 0
		while attempts < 3 and not success:
			try:
				self.cur.execute(cat_query)
				complete = False
				while not complete:
					row = self.cur.fetchone()
					if not row:
						complete = True
						continue
					cats.append(row['cl_to'])
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)
		return cats

	'''
	Method that returns all the babel users that has the babel template.
	'''
	def get_babel_template_users(self):
		logging.info("Collecting babel users")
		babel_user_query = ur'''SELECT /*SLOW_OK LIMIT: 1800 */ page_id, page_title FROM page p JOIN templatelinks tl ON page_id=tl_from WHERE tl_title = 'Babel' 
								AND tl.tl_namespace=10 and page_namespace = 2 {limit}'''
		bot_users = self.get_bot_users()
		success = False
		attempts = 0
		while attempts < 3 and not success:
			try:
				self.cur.execute(babel_user_query.format(limit = self.limit))
				for babel_user in self.cur.fetchall(): 
					print babel_user['page_id'], babel_user['page_title'], self.language
				success = True
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)

	def get_templates_sub_user_pages(self, dialectList, ISOList, langList):
		logging.info("Processing users that has templates in sub user pages")


		spanish_lang_cats = None
		if self.language == 'es':
			spanish_lang_cats = self.get_spanish_lang_categories()

		### Regular expression to catch language codes
		lang_regx = re.compile(ur'(?P<locUserCat>\w+)[_]+(?P<lcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		### Regular expression to catcch dialect codes
		dt_regx = re.compile(ur'(?P<locUserCat>\w+)[-]+(?P<dcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		### user without proficiency level (Sometimes en-0 gets put in to just en category)
		lang_regx_short = re.compile(ur'(?P<locUserCat>\w+)[_]+(?P<lcode>\w+)$', re.I | re.U)
	

		# Example: http://no.wikipedia.org/wiki/Bruker:Axezz/Presentasjon
		subpage_user_query = ur'''SELECT /* SLOW_OK LIMIT: 1800 */
			page_title, page_id, cl_to, page_namespace
			FROM page
			INNER JOIN categorylinks
			ON (page_id=cl_from)
			WHERE cl_to LIKE "{localized_usr}\_%" OR cl_to LIKE "User\_%"
			AND page_namespace=2 {limit}'''.format(localized_usr=self.translatedString, limit=self.limit)
		#print subpage_user_query
		# get user id from user_name
		query_user_id = ur'''SELECT user_id from user where user_name = "{uName}"'''
		success = False
		attempts = 0
		user_name_template_dict = {}
		bot_users = self.get_bot_users()
		while attempts < 3 and not success:
			try:
				self.cur.execute(subpage_user_query.encode('utf-8'))
				complete = False
				while not complete:
					row = self.cur.fetchone()
					if not row:
						complete = True
						continue
					page_title = unicode(row['page_title'], 'utf-8')
					template_category = unicode(row['cl_to'], 'utf-8')
					page_namespace = int(row['page_namespace'])
					
					'''
					Matching the regular expression of dialect template and language template
					'''
					matched_template_object = lang_regx.match(template_category)
					matched_dialect_object = dt_regx.match(template_category)
					matched_template_no_prof = lang_regx_short.match(template_category)

					query_user_id = ur'''SELECT user_id FROM user WHERE user_name = %(uName)s'''
					if page_namespace == 2:
						user_name = page_title.split("/")[0]
						formatted_user_name = user_name.replace("_", " ")
						self.cur2.execute(query_user_id, {'uName':formatted_user_name.encode('utf-8')})
						u_id = self.cur2.fetchone()
						self.cur2.fetchall()
						if u_id is not None and u_id not in bot_users:
							if self.language == 'es':
								stript_template = template_category.split("Wikipedia:Wikipedistas_con_")
								if len(stript_template) > 1:
									set_template = stript_template[1]
								else:
									set_template = template_category
								if (u_id['user_id'],user_name) not in user_name_template_dict:
									user_name_template_dict[(u_id['user_id'], user_name)] = []
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
								else:
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
							elif self.language == 'ast':
								stript_template = template_category.split('Usuarios_por_idioma_-_')
								if len(stript_template) > 1:
									set_template = stript_template[1]
								else:
									set_template = template_category
								if (u_id['user_id'],user_name) not in user_name_template_dict:
									user_name_template_dict[(u_id['user_id'], user_name)] = []
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
								else:
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
							elif self.language == 'ay':
								stript_template = template_category.split("Aru:")
								if len(stript_template) > 1:
									set_template = stript_template[1]
								else:
									set_template = template_category
								if (u_id['user_id'],user_name) not in user_name_template_dict:
									user_name_template_dict[(u_id['user_id'], user_name)] = []
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
								else:
									user_name_template_dict[(u_id['user_id'], user_name)].append(set_template)
							#If template regex doesn't return None then we have a successful match -> Lets process
							elif matched_template_object is not None:
								matched_lang_code = matched_template_object.group('lcode')
								'''Check if the language codes are in the langauge code list and ISO list'''
								if matched_lang_code in langList or matched_lang_code in ISOList:
									stript_template = template_category.split("_")[1]
									if (u_id['user_id'],user_name) not in user_name_template_dict:
										user_name_template_dict[(u_id['user_id'], user_name)] = []
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)
									else:
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)
							elif matched_template_no_prof is not None:
								matched_lang_without_proficiency = matched_template_no_prof.group('lcode')
								if matched_lang_without_proficiency in langList or matched_lang_without_proficiency in ISOList:
									stript_template = template_category.split("_")[1]
									if (u_id['user_id'],user_name) not in user_name_template_dict:
										user_name_template_dict[(u_id['user_id'], user_name)] = []
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)
									else:
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)
							# Matching dialects
							elif matched_dialect_object is not None:
								matched_dialect_code = matched_dialect_object.group('dcode')
								if matched_dialect_code in dialectList:
									if (u_id['user_id'],user_name) not in user_name_template_dict:
										user_name_template_dict[(u_id['user_id'], user_name)] = []
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)
									else:
										user_name_template_dict[(u_id['user_id'], user_name)].append(stript_template)	
						else:
							logging.info("User with " + user_name + " doesn't have a user_id or its a bot in wiki" + self.language)
					else:
						logging.info("In wiki-lang" + self.language + " " + page_title + " has template " + template_category + " and namespace is " + str(page_namespace))					
				success = True
				#self.cur.fetchall()
			except Exception, e:
				attempts += 1
				traceback.print_exc()
				logging.exception(e)
			except mdb.OperationalError, ex:
				logging.info("=== Caught database gone away === ")
				logging.exception(ex)
				self.disconnect_database()
				self.connect_database()
		return user_name_template_dict


	def get_all_template_users(self):
		logging.info("Collecting all template users")

		### Regular expression to catch language codes
		lang_regx = re.compile(ur'(?P<locUserCat>\w+)[_]+(?P<lcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		### Regular expression to catcch dialect codes
		dt_regx = re.compile(ur'(?P<locUserCat>\w+)[-]+(?P<dcode>\w+)[-]+(?P<level>[0-5\w])$', re.I | re.U)
		### user without proficiency level (Sometimes en-0 gets put in to just en category)
		lang_regx_short = re.compile(ur'(?P<locUserCat>\w+)[_]+(?P<lcode>\w+)$', re.I | re.U)		

		upperlanguage = self.language[0].upper() + self.language[1:]
		upperlanguage = unicode(upperlanguage, 'utf-8')

		if self.language == 'nl':
			template_user_query =  ur'''SELECT /* SLOW_OK LIMIT: 1800 */ user_id, user_name, cl_to 
				FROM page
				INNER JOIN categorylinks ON (page_id=cl_from)
				INNER JOIN user ON REPLACE(page_title, "_"," ") = user_name
				WHERE (cl_to LIKE "{User}\_%" OR cl_to LIKE "User\_%")
				AND page_namespace=2 {limit}'''.format(limit=self.limit, User="Wikipedia:"+self.translatedString)
		elif self.language == 'ay':
			template_user_query =  ur'''SELECT /* SLOW_OK LIMIT: 1800 */ user_id, user_name, cl_to 
				FROM page
				INNER JOIN categorylinks ON (page_id=cl_from)
				INNER JOIN user ON REPLACE(page_title, "_"," ") = user_name
				WHERE (cl_to LIKE "{User}" OR cl_to LIKE "User\_%")
				AND page_namespace=2 {limit}'''.format(limit=self.limit, User=self.translatedString)			
		else:			
			template_user_query =  ur'''SELECT /* SLOW_OK LIMIT: 1800 */ user_id, user_name, cl_to 
				FROM page
				INNER JOIN categorylinks ON (page_id=cl_from)
				INNER JOIN user ON REPLACE(page_title, "_"," ") = user_name
				WHERE (cl_to LIKE "{User}\_%" OR cl_to LIKE "User\_%")
				AND page_namespace=2 {limit}'''.format(limit=self.limit, User=self.translatedString)

		#print template_user_query	
		### Read the pipe file to get all the dialect codes and conver them to unicoded list	
		pipe_file = os.path.expanduser('~/template_data/lang_codes/ISO-639-2.csv')
		dialect_list = getDialectCodes(pipe_file)
		unicode_dialect_list = [unicode(x, 'utf-8') for x in dialect_list]

		# Convert Langauge List to unicode
		file_path = os.path.expanduser('~/template_data/lang_codes/wiki_lang2_codes.csv')
		all_lang_codes = getLanguageCodes(file_path)
		unicoded_list = [unicode(x, 'utf-8') for x in all_lang_codes]

		# Get ISO Codes
		iso_file_path = os.path.expanduser('~/template_data/lang_codes/iso_lang_codes.csv')
		iso_list = getLanguageCodes(iso_file_path)
		unicode_iso_list = [unicode(x, 'utf-8') for x in iso_list]

		try:
			sub_page_user_dictionary = self.get_templates_sub_user_pages(unicode_dialect_list, unicode_iso_list, unicoded_list)
		except AttributeError, e:
			logging.exception(e)

		data_file = open(self.filename, "wt")
		bot_users = self.get_bot_users()
		success = False
		attempts = 0
		with open(os.path.expanduser(self.filename), 'w+') as data_file:
			writer = csv.writer(data_file)
			writer.writerow(('user_id', 'user_name', 'template'))
			if sub_page_user_dictionary:
				logging.info("---+ writing subpage users dictionary +---")
				for key, values in sub_page_user_dictionary.iteritems():
					for value in  values:
						try:
							writer.writerow((key[0], key[1].encode('utf-8'), value.encode('utf-8')))
						except Exception, e:
							logging.error("Something went wrong in writing user_id " + str(key[0]) + " " + self.language)
			while attempts < 3 and not success:
				try:
					self.cur.execute(template_user_query.encode('utf-8'))
					complete = False
					while not complete:
						row = self.cur.fetchone()
						if not row:
							complete = True
							continue
						#for row in self.cur:
						if row['user_id'] not in bot_users:
							uID = row['user_id']
							template_category = unicode(row['cl_to'], 'utf-8')
							### Matching the language codes regular expression
							### nl has word wikipedia infront of the template, we could write regex to pick that then 
							### we might end up picking up garbage from other languages.
							matched_template_object = None
							matched_template_no_prof = None
							if self.language =='nl':
								matched_template_object = lang_regx.match(template_category.split(":")[1])
								# Add the condition as a data cleanup during if we capture en-1 we don't need to capture en without level
								# Some languages native is consider as just the main lang code ex: en-N is equivalent to en
								if matched_template_object is None:
									matched_template_no_prof = lang_regx_short.match(template_category.split(":")[1])
							else:
								matched_template_object = lang_regx.match(template_category)
								matched_template_no_prof = lang_regx_short.match(template_category)
							
							if matched_template_object is not None:
								matched_lang_code = matched_template_object.group('lcode')
								if matched_lang_code in unicoded_list or matched_lang_code in unicode_iso_list:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))

							''' Matching dialect code regular expression '''
							matched_dialect_object = dt_regx.match(template_category)
							if matched_dialect_object is not None:
								matched_dialect_code = matched_dialect_object.group('dcode')
								if matched_dialect_code in unicode_dialect_list:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))

							if matched_template_no_prof is not None:
								matched_lang_without_proficiency = matched_template_no_prof.group('lcode')
								if matched_lang_without_proficiency in unicoded_list or matched_lang_without_proficiency in unicode_iso_list:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))

							if self.language == 'es':
								if uID not in sub_page_user_dictionary:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									if "Wikipedia:Wikipedistas_con_" in template_code:
										stript_template = template_code.split("Wikipedia:Wikipedistas_con_")[1]
										writer.writerow((row['user_id'], user_name.encode('utf-8'), stript_template.encode('utf-8')))
									else:
										print template_code
										writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))

							if self.language == 'ast':
								if uID not in sub_page_user_dictionary:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									if "Usuarios_por_idioma_-_" in template_code:
										stript_template = template_code.split("Usuarios_por_idioma_-_")[1]
										writer.writerow((row['user_id'], user_name.encode('utf-8'), stript_template.encode('utf-8')))
									else:
										print template_code
										logging.info(row['user_id'], self.language)
										writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))

							if self.language == 'ay':
								if uID not in sub_page_user_dictionary:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									if "Aru:" in template_code:
										stript_template = template_code.split("Aru:")[1]
										writer.writerow((row['user_id'], user_name.encode('utf-8'), stript_template.encode('utf-8')))
									else:
										print template_code
										writer.writerow((row['user_id'], user_name.encode('utf-8'), template_code.encode('utf-8')))
							if self.language == 'ang':
								if uID not in sub_page_user_dictionary:
									user_name = unicode(row['user_name'], 'utf-8')
									template_code = unicode(row['cl_to'], 'utf-8')
									stript_template = template_code.split(u"_")[1]
									writer.writerow((row['user_id'], user_name.encode('utf-8'), stript_template.encode('utf-8')))									
						else:
							logging.info(str(row['user_id']) + " is a bot " + " wiki " + self.language)
					success = True
				except Exception, e:
					attempts += 1
					traceback.print_exc()
					logging.exception(e)
				except mdb.OperationalError, ex:
					logging.info("==== Database Gone away ====")
					logging.exception(ex)
					self.disconnect_database()
					self.connect_database

def main():
	log = logging.getLogger()

	parser = argparse.ArgumentParser()
	parser.add_argument('lang_file', type=str, help='which language file to read in as data collection')
	args = parser.parse_args()

	create_logger(log, args.lang_file)

	if args.lang_file == 'test':
		language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/test_lang.csv"))
	elif args.lang_file == 'top50':
		language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/top_50_langs.csv"))
	elif args.lang_file == 'second50':
		language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/second_50_langs.csv"))
	elif args.lang_file == 'after100':
		language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/after_100_langs.csv"))
	elif args.lang_file == 'all':
		language_list = getLanguageCodes(os.path.expanduser("~/template_data/lang_codes/wiki_lang_codes_latest.csv"))

	for language_code in language_list:
		babelUsers = CollectUsersWithTemplate(language_code, '')
		#babelUsers.get_babel_template_users()
		#import pdb
		#pdb.set_trace()
		babelUsers.get_all_template_users()

if __name__=="__main__":
	main()