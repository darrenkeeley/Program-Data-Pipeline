"""
This script runs several queries across three databases (TD, CAS and CRCR), saves the results as tab delim txt's, then uploads them to a secure file transfer site.
It iterates through the Queries folder and will stop if one of them doesn't succeed.
"""

#%%
import os
import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
from datetime import datetime
import subprocess

#%% Setup working directory to be same as script location.
# For reference, my script folder is 'C:\db to sft pipeline'
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
print('Current working directory: {}'.format(dname))

#%% Create DataStaging folder in working directory if it does not exist. This is where the data is downloaded to as delim txt.
if not os.path.exists('DataStaging'):
	os.makedirs('DataStaging')
	print('Created DataStaging folder because it was not found\n')
	
#%% Read configuration file for filepaths, usernames and passwords.
cfg = ET.parse('config.xml').getroot()

#%% Open OBDC connections.
td_cnxn = pyodbc.connect('DSN=td;UID={},PWD={};'.format(cfg.find('td/username').text, cfg.find('td/password').text))
cas_cnxn = pyodbc.connect('DSN=cas;UID={},PWD={};'.format(cfg.find('cas/username').text, cfg.find('cas/password').text))
crcr_cnxn = pyodbc.connect('DSN=crcr;UID={},PWD={};'.format(cfg.find('crcr/username').text, cfg.find('crcr/password').text))

#%% Function that runs a query and saves the result as delim txt.
def dl_data(program_name, data_name, query_filepath, cnxn):
	try:
		# Create program folder
		if not os.path.exists(r'DataStaging\{}'.format(program_name)):
			os.makedirs(r'DataStaging\{}'.format(program_name))
			
		# Read query from sql file
		print(data_name + " download started at " + datetime.now().strftime("%H:%M"))
		f = open(query_filepath,'r')
		query = f.read()
		
		# Download to dataframe
		df = pd.read_sql(query, cnxn)
		f.close()
		
		# Save results as txt
		df.to_csv(r"DataStaging\{}\{}.txt".format(program_name, data_name), index=False, sep='\t')
		print("Finished at {}. Dimensions {}\n".format(datetime.now().strftime("%H:%M"), df.shape))
		
	except Exception as e:
		raise e
		
#%% Iterate through all queries
queries_root_directory = os.path.join(dname,"Queries")

for program_name in os.listdir(queries_root_directory):
	for cnxn_name in os.listdir(os.path.join(queries_root_directory, program_name)):
		for data_name_sql in os.listdir(os.path.join(queries_root_directory, program_name, cnxn_name)):
			if data_name_sql.endswith('.sql'):
				# Find connection
				if cnxn_name=='td': cnxn=td_cnxn
				elif cnxn_name=='cas': cnxn=cas_cnxn
				elif cnxn_name=='crcr': cnxn=crcr_cnxn
				
				query_filepath = os.path.join(queries_root_directory, program_name, cnxn_name, data_name_sql)
				
				# Call download function. Loop is in case connection is idle too long and disconnects.
				for attempt in range(2):
					try:
						dl_data(data_name=os.path.split(data_name_sql)[0],
							program_name=program_name,
							query_filepath=query_filepath,
							cnxn=cnxn)
					except:
						# Reconnect
						print('Reconnecting and attempting again...')
						if cnxn_name=='td':
							cnxn = pyodbc.connect('DSN=td;UID={},PWD={};'.format(cfg.find('td/username').text, cfg.find('td/password').text))
						elif cnxn_name=='cas':
							cnxn = pyodbc.connect('DSN=cas;UID={},PWD={};'.format(cfg.find('cas/username').text, cfg.find('cas/password').text))
						elif cnxn_name=='crcr':
							cnxn = pyodbc.connect('DSN=crcr;UID={},PWD={};'.format(cfg.find('crcr/username').text, cfg.find('crcr/password').text))
					else:
						break
			else:
				continue

#%% Close connections
try:
	td_cnxn.close()
	cas_cnxn.close()
	crcr_cnxn.close()

except Exception as e: print(e)

#%% Upload files to SFT
try:
	print("SFT sync started at " + datetime.noew().strftime("%H:%M"))
	
	p = subprocess.run(["RunWinscpScript.bat",
							cfg.fin('paths/winscp_filepath').text,
							os.path.join(dname,'WinscpScript.txt'),
							cfg.find('sft/username').text,
							cfg.find('sft/password').text,
							os.path.join(dname,'DataStaging'),
							cfg.find('paths/sft_filepath').text],
						shell=True, stdout=subprocess.PIPE)
	
	print("Finished at {}.\n".format(datetime.noew().strftime("%H:%M")))
	
except Exception as e:
	raise(e)
	
#%% Cleanup
del cfg