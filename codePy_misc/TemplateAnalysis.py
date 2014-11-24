import os, sys
import csv
from os import listdir
from os.path import isfile, join

#mypath = os.path.expanduser("~/GroupLensResearch/WikipediaResearch/Multi_Ling_Wiki/multilingual-wikipedians/template_analysis/data_2014_03_27/")
mypath = os.path.expanduser("~/outputs/data_2014_03_27/")

print "template_and_proficiency, # of users wiki of data collected from"
#print "Wiki", "# of template users"
completed_file = []
for filename in os.listdir(mypath):
	if filename not in completed_file:
		completed_file.append(filename)
		file_location = os.path.expanduser(mypath + filename)
		input_file = csv.DictReader(open(file_location))

		user_template_count = {}
		unique_template_per_language = {}
		for row in input_file:
			if "ActionScript" not in row["template"]:
				if row["user_id"] not in user_template_count:
					user_template_count[row["user_id"]] = []
					user_template_count[row["user_id"]].append(row["template"])
				else:
					user_template_count[row["user_id"]].append(row["template"])

				if row["template"] not in unique_template_per_language:
					unique_template_per_language[row["template"]] = []
					unique_template_per_language[row["template"]].append(row["user_id"])
				else:
					unique_template_per_language[row["template"]].append(row["user_id"])
		## Calculating number of unique users using templates			
		#count = 0
		#for key,value in user_template_count.iteritems():
		#	#print key, len(value)
		#	count += 1
		#print filename.split("-")[0], ",", count
		with open(os.path.expanduser(filename), 'w+') as data_file:
			for key,value in user_template_count.items():
				writer = csv.writer(data_file)
				writer.writerow([key, value])
		#if filename.split("-")[0] == 'war':
		#	for key,value in unique_template_per_language.iteritems():
		#		print key, ",", len(value)