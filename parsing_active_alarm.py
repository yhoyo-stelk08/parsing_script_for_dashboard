import os
import re
import pandas as pd
import numpy as np
import json
import zipfile
import sys

from datetime import datetime,timedelta
from add_func import export_to_csv,setCurDir,readConfigFile



# def setDfActiveAlarmEms():
# 	curdir = setCurDir()
# 	df_alarm = pd.read_csv(curdir+os.sep+'active_alarm'+os.sep+'active_alarm_ems.txt',sep='|',lineterminator='\n')
	# return df_alarm

def setDfActiveAlarmUme(ume):
	df_res = pd.DataFrame()
	if ume == "UME_KAL" :
		filedir = "/home/zte_ftp/tmp/bot_files_ume/FM/kal"
		fileres = "active_alarm_ume_kal.csv"
		df_alarm = pd.read_csv(filedir+os.sep+sys.argv[1],sep=',',lineterminator='\n',quotechar='"',dtype=str)

	elif ume == "UME_SUL" :
		filedir = "/home/zte_ftp/tmp/bot_files_ume/FM/sul"
		fileres = "active_alarm_ume_sul.csv"
		df_alarm = pd.read_csv(filedir+os.sep+sys.argv[1],sep=',',lineterminator='\n',quotechar='"',dtype=str)

	elif ume == "UME_PUMA" :
		filedir = "/home/zte_ftp/tmp/bot_files_ume/FM/puma"
		fileres = "active_alarm_ume_puma.csv"
		df_alarm = pd.read_csv(filedir+os.sep+sys.argv[1],sep=',',lineterminator='\n',quotechar='"',dtype=str)
	else :
		print('Wrong Parameter')

	df_alarm = df_alarm.replace(r'\r+|\n+',';', regex=True)
	df_res['No'] = ''
	df_res['me'] = df_alarm['ME']
	df_res['DN'] = df_alarm['DN']
	df_res['me_name'] = df_alarm['ME']
	df_res['related_me_name'] = df_alarm['Related ME Name']
	df_res['alarm_severity'] = df_alarm['Alarm Severity']
	df_res['alarm_category'] = df_alarm['Alarm Type']
	df_res['alarm_name'] = df_alarm['Alarm Code Name'].astype(str)+'('+df_alarm['Alarm Code'].astype(str)+')'
	# df_res['alarm_desc'] = df_alarm['Specific Problem']
	df_res['raised_time'] = df_alarm['Occurrence Time'].str[:4]+df_alarm['Occurrence Time'].str[5:7]+df_alarm['Occurrence Time'].str[8:10]+' '+df_alarm['Occurrence Time'].str[11:19]
	df_res['me_ip'] = df_alarm['ME IP']
	df_res['moc'] = df_alarm['MOC']
	df_res['additional_info'] = df_alarm['Additional Parameters']
	df_res['alarm_id'] = df_alarm['Alarm ID']
	df_res['product'] = df_alarm['Product']
	df_res['me_id'] = df_alarm['ME ID']
	df_res['board_type'] = df_alarm['Board Type']
	df_res['alarm_object'] = df_alarm['Alarm Object']
	df_res['position'] = df_alarm['Position']
	
	df_res.to_csv(filedir+os.sep+fileres, encoding='utf-8',index=False,sep='|')
	print(df_res)

setDfActiveAlarmUme(sys.argv[2])

