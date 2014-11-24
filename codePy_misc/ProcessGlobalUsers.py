import os
import sys
import csv
import pandas as pd
import pdb

def get_pandas_df(fileLocation):
	df = pd.read_csv(fileLocation)
	return df

def get_non_en_contributors(df):
	print 'hell0'

def main():

	with open(os.path.expanduser('/Users/anu/GroupLensResearch/WikiResearch/data/Users.csv'), 'w+') as data_file:
		writer = csv.writer(data_file)
		writer.writerow(('lang','level_1', 'level_2', 'level_3', 'level_4', 'level_5', 'level_N'))
		for filename in os.listdir('/Users/anu/GroupLensResearch/WikiResearch/data/edits/'):
			lang = filename.split('_')[0]
			print lang
			df = get_pandas_df('/Users/anu/GroupLensResearch/WikiResearch/data/edits/'+lang+'_global_proficency_editcount.csv')
			#df[(df['home_wiki']!='en') & (df['edit_count']==0)]
			#en1-2493, en2-6696, en3-6811, en4-1591, en5-202, enN-248
			#len(df[(df['home_wiki'] =='en') & (df['edit_count']==0) & (df['proficency_level']=='en-1')])
			writer.writerow([lang, len(df[(df['home_wiki'] == lang) & (df['proficency_level']==lang+'-1')]), \
				len(df[(df['home_wiki'] ==lang) & (df['proficency_level']==lang+'-2')]), \
				len(df[(df['home_wiki'] ==lang) & (df['proficency_level']==lang+'-3')]),\
				len(df[(df['home_wiki'] ==lang) & (df['proficency_level']==lang+'-4')]),\
				len(df[(df['home_wiki'] ==lang) & (df['proficency_level']==lang+'-5')]),\
				len(df[(df['home_wiki'] ==lang) & (df['proficency_level']==lang+'-N')])])
	#pdb.set_trace()

if __name__ == '__main__':
	main()