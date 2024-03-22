import os
import re
import pandas as pd
import numpy as np
import json
import zipfile

from datetime import datetime,timedelta
from add_func import export_to_csv,setCurDir,readConfigFile

def setDfPayload2gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"2G"
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+ABSCCELLPSBASICMEAS'+'.+',file)
		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename,thousands=',')
			df_res['tech'] = '2G'
			df_res['OSS'] = ems
			df_res['payload_mbyte'] = (((df_res['C900040190'])*176+(df_res['C900040191'])*224+(df_res['C900040192'])*296+(df_res['C900040193'])*352+(df_res['C900040194'])*448+(df_res['C900040195'])*592+(df_res['C900040196'])*448+(df_res['C900040197'])*544+(df_res['C900040198'])*592)/1024/1024/8)+(((df_res['C900040217'])*176+(df_res['C900040218'])*224+(df_res['C900040219'])*296+(df_res['C900040220'])*352+(df_res['C900040221'])*448+(df_res['C900040222'])*592+(df_res['C900040223'])*448+(df_res['C900040224'])*544+(df_res['C900040225'])*592)/1024/1024/8)+(((df_res['C900040089']*160+df_res['C900040090']*240+df_res['C900040091']*288+df_res['C900040092']*400)+(df_res['C900040102']*160+df_res['C900040103']*240+df_res['C900040104']*288+df_res['C900040105']*400))/1024/1024/8)
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'IBSCMEID'
					,'SITEID'
					,'BTSID'
					,'OSS'
					,'tech'
					,'payload_mbyte'
				]
			]
			df_result.rename(columns={
				'IBSCMEID' : 'CONTROLLERID'
				,'BTSID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfPayload2gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Payload2G'][0]['src_dir'] + os.sep + dirdate
	extract_to_dir = curdir+os.sep+ume+os.sep+'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

	df_result = pd.DataFrame()

	if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":
		# change directory to filedir directory
		os.chdir(filedir)
		# list all files inside filedir directory
		list_file = os.listdir(filedir)

		# delete all files under dir band
		for file in os.listdir(extract_to_dir) :
			# shutil.rmtree(file)
			os.remove(extract_to_dir+os.sep+file)
			# print(file)
		
		# extract files from filedir
		for file in list_file :
			if ume == "UME_SUL" :
				zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_kal']
			# print(filedatetime)
			pattern = zipFileName+'_'+filedatetime
			# print(pattern)
			result = re.search(pattern+'.+',file)
			if result :
				print(result.group(0))
				filename = result.group(0)
				print('Extracting file '+filename)
				with zipfile.ZipFile(filedir+os.sep+filename, 'r') as zip_ref:
					zip_ref.extractall(extract_to_dir+os.sep)
					print("File "+filename+" telah di extract ke "+extract_to_dir)

		# delete unneeded files
		for file in os.listdir(extract_to_dir) :
			result = re.search('.+kpis.+',file)
			if result :
				# print(result.group(0))
				# delete file kpis in extract_to_dir
				kpis_files = result.group(0)
				os.remove(extract_to_dir+os.sep+kpis_files)
				
		# set dataframe process
		for file in os.listdir(extract_to_dir) :
			# print(file)
			df_res = pd.read_csv(extract_to_dir+os.sep+file,thousands=',')
			df_res['GRANULARITY'] = 900
			df_res['tech'] = '2G'
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)
			# print(df_res['COLLECTTIME'])
			if ume == "UME_SUL" :
				df_result = df_res[
					[
						'COLLECTTIME'
						,'GRANULARITY'
						,'SubnetWork ID'
						,'SITE ID'
						,'BTS ID'
						,'OSS'
						,'tech'
						,'TOTAL PAYLOAD (EDGE+GPRS)'
					]
				]
				df_result.rename(columns={
					'SubnetWork ID' : 'CONTROLLERID'
					,'SITE ID' : 'SITEID'
					,'BTS ID' : 'CELLID'
					,'TOTAL PAYLOAD (EDGE+GPRS)' : 'payload_mbyte'
				},inplace=True)
			elif ume == "UME_KAL" :
				df_result = df_res[
					[
						'COLLECTTIME'
						,'GRANULARITY'
						,'SubnetWork ID'
						,'SITE ID'
						,'BTS ID'
						,'OSS'
						,'tech'
						,'TOTAL PAYLOAD (EDGE+GPRS)'
					]
				]
				df_result.rename(columns={
					'SubnetWork ID' : 'CONTROLLERID'
					,'SITE ID' : 'SITEID'
					,'BTS ID' : 'CELLID'
					,'TOTAL PAYLOAD (EDGE+GPRS)' : 'payload_mbyte'
				},inplace=True)
			# return df_res
	else :
		print('Enter either UME_SUL or UME_KAL or UME_PUMA')
	
	# df_result['availability'] = df_result['availability'].str.rstrip('%').astype('float')/100
	return df_result

def setDfPayload3gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"3G"
	list_file = os.listdir(file_dir)
	df_res1 = pd.DataFrame()
	df_res2 = pd.DataFrame()
	for file in list_file :
		result1 = re.search('.+AR9CELLFLUX'+'.+',file)
		result2 = re.search('.+AR9CELLCOMMFLUX'+'.+',file)

		if result1 :
			# print(result1.group(0))
			filename1 = result1.group(0)
			df1 = pd.read_csv(file_dir+os.sep+filename1,thousands=',')
			df_res1 = df1[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'RNCID'
					,'NODEBID'
					,'OBJECTID'
					,'C310010146'
					,'C310010147'
					,'C310010148'
					,'C310010150'
					,'C310010151'
					,'C310010152'
					,'C310010158'
					,'C310010159'
					,'C310010160'
					,'C310010154'
					,'C310010155'
					,'C310010156'
				]
			]
			df_res1['primKey'] = df_res1['RNCID'].astype(str)+df_res1['NODEBID'].astype(str)+df_res1['OBJECTID'].astype(str)
	
		if result2 :
			# print(result2.group(0))
			filename2 = result2.group(0)
			df2 = pd.read_csv(file_dir+os.sep+filename2)
			df_res2 = df2[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'RNCID'
					,'NODEBID'
					,'OBJECTID'
					,'C310053371'
					,'C310053377'
				]
			]
			df_res2['primKey'] = df_res2['RNCID'].astype(str)+df_res2['NODEBID'].astype(str)+df_res2['OBJECTID'].astype(str)
		
	df_merge = df_res1.merge(df_res2,on='primKey',how='inner')
	df_merge.rename(columns={
		'COLLECTTIME_x' : 'COLLECTTIME'
		,'GRANULARITY_x' : 'GRANULARITY'
		,'RNCID_x' : 'CONTROLLERID'
		,'NODEBID_x' : 'SITEID'
		,'OBJECTID_x' : 'CELLID'
	},inplace=True)
	df_merge.fillna('#N/A',inplace=True)
	df_merge['payload_mbyte'] = ((df_merge['C310010146']+df_merge['C310010147']+df_merge['C310010148']+df_merge['C310053371']+df_merge['C310010150']+df_merge['C310010151']+df_merge['C310010152']+df_merge['C310053377'])+(df_merge['C310010158']+df_merge['C310010159']+df_merge['C310010160'])+(df_merge['C310010154']+df_merge['C310010155']+df_merge['C310010156']))/1024
	df_merge['tech'] = '3G'
	df_merge['OSS'] = ems
	df_result = df_merge[
		[
			'COLLECTTIME'
			,'GRANULARITY'
			,'CONTROLLERID'
			,'SITEID'
			,'CELLID'
			,'OSS'
			,'tech'
			,'payload_mbyte'
		]
	]

	return df_result

def setDfPayload3gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Payload3G'][0]['src_dir'] + os.sep + dirdate
	extract_to_dir = curdir+os.sep+ume+os.sep+'Trf_Thp_Paging_Avail_Pyld'+os.sep+'3G'

	df_result = pd.DataFrame()

	if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":
		# change directory to filedir directory
		os.chdir(filedir)
		# list all files inside filedir directory
		list_file = os.listdir(filedir)

		# delete all files under dir band
		for file in os.listdir(extract_to_dir) :
			# shutil.rmtree(file)
			os.remove(extract_to_dir+os.sep+file)
			# print(file)
		
		# extract files from filedir
		for file in list_file :
			if ume == "UME_SUL" :
				zipFileName = config_data['SRC_UME']['Payload3G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['Payload3G'][0]['prefix_fname_kal']
			# print(filedatetime)
			pattern = zipFileName+'_'+filedatetime
			# print(filename)
			result = re.search(pattern+'.+',file)
			if result :
				# print(result.group(0))
				filename = result.group(0)
				print('Extracting file '+filename)
				with zipfile.ZipFile(filedir+os.sep+filename, 'r') as zip_ref:
					zip_ref.extractall(extract_to_dir+os.sep)
					print("File "+filename+" telah di extract ke "+extract_to_dir)

		# delete unneeded files
		for file in os.listdir(extract_to_dir) :
			result = re.search('.+kpis.+',file)
			if result :
				# print(result.group(0))
				# delete file kpis in extract_to_dir
				kpis_files = result.group(0)
				os.remove(extract_to_dir+os.sep+kpis_files)

		# set dataframe process
		for file in os.listdir(extract_to_dir) :
			# print(file)
			df_res = pd.read_csv(extract_to_dir+os.sep+file,thousands=',')
			df_res['GRANULARITY'] = 900
			df_res['tech'] = '3G'
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)

			# print(df_res['COLLECTTIME'])
			if ume == "UME_SUL" :
				# df_res['HSDPA Payload (Mbyte) NFJ-TSEL'] = df_res['HSDPA Payload (Mbyte) NFJ-TSEL'].str.replace(',','').astype(float)
				df_res['payload_mbyte'] = df_res['KPI 3G PS Payload (Mbyte)'] + df_res['HSDPA Payload (Mbyte) NFJ-TSEL'] + df_res['HSUPA Payload (Mbyte) NFJ_1520165243750-3']
				df_result = df_res[
					[
						'COLLECTTIME'
						,'GRANULARITY'
						,'SubnetWork ID'
						,'NodeB ID'
						,'Cell ID'
						,'OSS'
						,'tech'
						,'payload_mbyte'
					]
				]
				df_result.rename(columns={
					'SubnetWork ID' : 'CONTROLLERID'
					,'NodeB ID' : 'SITEID'
					,'Cell ID' : 'CELLID'
				},inplace=True)
			elif ume == "UME_PUMA" :
				# df_res['HSDPA Payload (Mbyte) NFJ-TSEL'] = df_res['HSDPA Payload (Mbyte) NFJ-TSEL'].str.replace(',','').astype(float)
				df_res['payload_mbyte'] = df_res['KPI 3G PS Payload (Mbyte)'] + df_res['HSDPA Payload (Mbyte) NFJ-TSEL'] + df_res['HSUPA Payload (Mbyte) NFJ_1520165243750-3']
				df_result = df_res[
					[
						'COLLECTTIME'
						,'GRANULARITY'
						,'SubnetWork ID'
						,'NodeB ID'
						,'Cell ID'
						,'OSS'
						,'tech'
						,'payload_mbyte'
					]
				]
				df_result.rename(columns={
					'SubnetWork ID' : 'CONTROLLERID'
					,'NodeB ID' : 'SITEID'
					,'Cell ID' : 'CELLID'
				},inplace=True)
			elif ume == "UME_KAL" :
				# df_res['HSDPA Payload (Mbyte) NFJ-TSEL_1638262476066'] = df_res['HSDPA Payload (Mbyte) NFJ-TSEL_1638262476066'].str.replace(',','').astype(float)
				df_res['payload_mbyte'] = df_res['KPI 3G PS Payload (Mbyte)'] + df_res['HSDPA Payload (Mbyte) NFJ-TSEL_1638262476066'] + df_res['HSUPA Payload (Mbyte) NFJ_1520165243750-3_1638262476243']
				df_result = df_res[
					[
						'COLLECTTIME'
						,'GRANULARITY'
						,'SubnetWork ID'
						,'NodeB ID'
						,'Cell ID'
						,'OSS'
						,'tech'
						,'payload_mbyte'
					]
				]
				df_result.rename(columns={
					'SubnetWork ID' : 'CONTROLLERID'
					,'NodeB ID' : 'SITEID'
					,'Cell ID' : 'CELLID'
				},inplace=True)
	else :
		print('Enter either UME_SUL or UME_KAL or UME_PUMA')

	# df_result['availability'] = df_result['availability'].str.rstrip('%').astype('float')/100
	return df_result

def setDfPayload4gEms(ems,band):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"4G"+os.sep+band
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+CELLTHRPUT_'+'.+',file)
		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename,thousands=',')
			df_res['tech'] = '4G '+band
			df_res['OSS'] = ems
			df_res['GRANULARITY'] = 900
			df_res['payload_mbyte'] = ((df_res['C373343806']*1000000+df_res['C373343807']*1000)/(8*1024*1024))+((df_res['C373343804']*1000000+df_res['C373343805']*1000)/(8*1024*1024))
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SBNID'
					,'ENODEBID'
					,'CellID'
					,'OSS'
					,'tech'
					,'payload_mbyte'
				]
			]
			df_result.rename(columns={
				'SBNID' : 'CONTROLLERID'
				,'ENODEBID' : 'SITEID'
				,'CellID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfPayload4gUme(ume,band):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Payload4G'][0]['src_dir'] + os.sep + dirdate
	extract_to_dir = curdir+os.sep+ume+os.sep+'Trf_Thp_Paging_Avail_Pyld'+os.sep+'4G'+os.sep+band

	df_result = pd.DataFrame()

	if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":
		# change directory to filedir directory
		os.chdir(filedir)
		# list all files inside filedir directory
		list_file = os.listdir(filedir)

		# delete all files under dir band
		for file in os.listdir(extract_to_dir) :
			# shutil.rmtree(file)
			os.remove(extract_to_dir+os.sep+file)
			# print(file)

		# extract files from filedir
		for file in list_file :
			if band == "FDD" :
				if ume == "UME_SUL" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_sul']
				elif ume == "UME_PUMA" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_puma']
				elif ume == "UME_KAL" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_kal']
			else :
				if ume == "UME_SUL" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_sul']
				elif ume == "UME_PUMA" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_puma']
				elif ume == "UME_KAL" :
					zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_kal']
			# print(filedatetime)
			pattern = zipFileName+'_'+filedatetime
			# print(pattern)
			result = re.search(pattern+'.+',file)
			if result :
				# print(result.group(0))
				filename = result.group(0)
				print('Extracting file '+filename)
				with zipfile.ZipFile(filedir+os.sep+filename, 'r') as zip_ref:
					zip_ref.extractall(extract_to_dir+os.sep)
					print("File "+filename+" telah di extract ke "+extract_to_dir)

		# delete unneeded files
		for file in os.listdir(extract_to_dir) :
			result = re.search('.+kpis.+',file)
			if result :
				# print(result.group(0))
				# delete file kpis in extract_to_dir
				kpis_files = result.group(0)
				os.remove(extract_to_dir+os.sep+kpis_files)

		for file in os.listdir(extract_to_dir) :
			# print(file)
			df_res = pd.read_csv(extract_to_dir+os.sep+file,thousands=",")
			df_res['GRANULARITY'] = 900
			df_res['tech'] = '4G ' + band
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)
			# print(df_res['COLLECTTIME'])
			if band == "FDD" :
				if ume == "UME_SUL" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_FDD_SDRITBBU'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_FDD_SDRITBBU' : 'payload_mbyte'
					},inplace=True)
				elif ume == "UME_PUMA" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_FDD_SDRITBBU'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_FDD_SDRITBBU' : 'payload_mbyte'
					},inplace=True)
				elif ume == "UME_KAL" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_FDD'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_FDD' : 'payload_mbyte'
					},inplace=True)
				else :
					print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
			elif band == "TDD" :
				if ume == "UME_SUL" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_TDD_SDRITBBU'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_TDD_SDRITBBU' : 'payload_mbyte'
					},inplace=True)
				elif ume == "UME_PUMA" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_TDD_SDRITBBU'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_TDD_SDRITBBU' : 'payload_mbyte'
					},inplace=True)
				elif ume == "UME_KAL" :
					df_result = df_res[
						[
							'COLLECTTIME'
							,'GRANULARITY'
							,'SubnetWork ID'
							,'eNodeBId'
							,'cellId'
							,'OSS'
							,'tech'
							,'4G_Payload_TDD'
						]
					]
					df_result.rename(columns={
						'SubnetWork ID' : 'CONTROLLERID'
						,'eNodeBId' : 'SITEID'
						,'cellId' : 'CELLID'
						,'4G_Payload_TDD' : 'payload_mbyte'
					},inplace=True)
				else :
					print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
			else :
				print('Band value only FDD or TDD')

	else :
		print('Enter either UME_SUL or UME_KAL or UME_PUMA')

	# df_result['payload_mbyte'] = df_result['payload_mbyte'].str.rstrip('%').astype('float')/100
	return df_result

def joining_df_2g3g4gEms(ems):
	df_pyld_2g = setDfPayload2gEms(ems)
	df_pyld_3g = setDfPayload3gEms(ems)
	df_pyld_fdd = setDfPayload4gEms(ems,"FDD")
	df_pyld_tdd = setDfPayload4gEms(ems,"TDD")

	df_result = pd.concat([df_pyld_2g,df_pyld_3g,df_pyld_fdd,df_pyld_tdd])

	return df_result

def joining_df_2g3g4gUme(ume):
	df_pyld_2g = setDfPayload2gUme(ume)
	# df_pyld_3g = setDfPayload3gUme(ume) # no more 3G
	df_pyld_fdd = setDfPayload4gUme(ume,"FDD")
	df_pyld_tdd = setDfPayload4gUme(ume,"TDD")

	# df_result = pd.concat([df_pyld_2g,df_pyld_3g,df_pyld_fdd,df_pyld_tdd])
	df_result = pd.concat([df_pyld_2g,df_pyld_fdd,df_pyld_tdd])

	return df_result

def parsing_payload():
	# no more EMS
	# df_ems5 = joining_df_2g3g4gEms("EMS5")
	# df_ems6 = joining_df_2g3g4gEms("EMS6")
	# df_ems7 = joining_df_2g3g4gEms("EMS7")
	df_ume_sul = joining_df_2g3g4gUme("UME_SUL")
	df_ume_kal = joining_df_2g3g4gUme("UME_KAL")
	df_ume_puma = joining_df_2g3g4gUme("UME_PUMA")
	# df_result = df_ems5
	df_result = pd.concat([df_ume_puma,df_ume_sul,df_ume_kal])
	df_result['primKey'] = df_result['CONTROLLERID'].astype(str)+df_result['SITEID'].astype(str)+df_result['CELLID'].astype(str)

	return df_result

def counting_payload(item):
	curdir = setCurDir()
	# delta_hour =  (datetime.today() - timedelta(hours=1,minutes=45))
	delta_hour =  (datetime.today() - timedelta(minutes=0))
	curdate = (delta_hour).strftime('%Y-%m-%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
	cur_datetime = curdate +' '+ qtime

	df_payload = parsing_payload()
	df_payload['datetime_id'] = cur_datetime
	df_payload['sitePrimKey'] = df_payload['CONTROLLERID'].astype(str)+df_payload['SITEID'].astype(str)
	df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
	df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(str)+df_data_poi['SITE_NUM'].astype(str)
	# return df_data_poi
	df_merge = df_payload.merge(df_data_poi,on='sitePrimKey',how='inner')
	df_merge['payload_gbyte'] = np.round((df_merge['payload_mbyte'])/1000,2)
	if item == "poi_name" :
		df_pivot = pd.pivot_table(df_merge,values='payload_gbyte',index=['datetime_id','POI_NAME','POI_LONGITUDE','POI_LATITUDE'],aggfunc=np.sum)
	elif item == "poi_location" :
		df_pivot = pd.pivot_table(df_merge,values='payload_gbyte',index=['datetime_id','POI_LOCATION'],aggfunc=np.sum)
	else : 
		df_pivot = pd.pivot_table(df_merge,values='payload_gbyte',index=['datetime_id','NSA'],aggfunc=np.sum)
	df_result = df_pivot.reset_index()
	return df_result


# print(counting_payload('poi_location'))
# print(counting_payload('poi_name'))
# print(counting_payload('NSA'))
# print(setDfDataPOI())
# df = parsing_payload()
# pivot = np.round(pd.pivot_table(df,values='payload_mbyte',index='',2)
# print(df)
# export_to_csv(df,'all_payload.csv')
