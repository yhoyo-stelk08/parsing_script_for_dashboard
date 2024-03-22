import os
import re
import pandas as pd
import numpy as np
import json
import zipfile

from datetime import datetime,timedelta
from add_func import export_to_csv,setCurDir,readConfigFile

def setDfPaging2gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"2G"
	list_file = os.listdir(file_dir)
	df_result = pd.DataFrame()
	for file in list_file :
		result = re.search('.+ABSCCSBASICMEAS'+'.+',file)
		
		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename)
			df_res['tech'] = '2G'
			df_res['OSS'] = ems
			df_res['paging'] = df_res['C900060152'] + df_res['C900060154']
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'IBSCMEID'
					,'SITEID'
					,'BTSID'
					,'OSS'
					,'tech'
					,'paging'
				]
			]
			df_result.rename(columns={
				'IBSCMEID' : 'CONTROLLERID'
				,'BTSID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfPaging2gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Paging2G'][0]['src_dir'] + os.sep + dirdate
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
				zipFileName = config_data['SRC_UME']['Paging2G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['Paging2G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['Paging2G'][0]['prefix_fname_kal']
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
			df_res = pd.read_csv(extract_to_dir+os.sep+file)
			df_res['GRANULARITY'] = 900
			df_res['tech'] = '2G'
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)
			# remove comma df paging columns
			df_res['Number of Abis interface paging command messages'] = df_res['Number of Abis interface paging command messages'].str.replace(',','').astype(float)
			df_res['Number of Abis PS paging messages'] = df_res['Number of Abis PS paging messages'].str.replace(',','').astype(float)
			# set paging
			df_res['paging'] = df_res['Number of Abis interface paging command messages']+df_res['Number of Abis PS paging messages']
			# print(df_res['COLLECTTIME'])
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SubnetWork ID'
					,'SITE ID'
					,'BTS ID'
					,'OSS'
					,'tech'
					,'paging'
				]
			]
			df_result.rename(columns={
				'SubnetWork ID' : 'CONTROLLERID'
				,'SITE ID' : 'SITEID'
				,'BTS ID' : 'CELLID'
			},inplace=True)
			# return df_res
	else :
		print('Enter either UME_SUL or UME_KAL or UME_PUMA')
	
	return df_result

def setDfPaging3gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"3G"
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+AR9CELLRRCCONNP'+'.+',file)

		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename)
			df_res['tech'] = '3G'
			df_res['OSS'] = ems
			df_res['paging'] = df_res['C310555152']
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'RNCID'
					,'NODEBID'
					,'OBJECTID'
					,'OSS'
					,'tech'
					,'paging'
				]
			]
			df_result.rename(columns={
				'RNCID' : 'CONTROLLERID'
				,'NODEBID' : 'SITEID'
				,'OBJECTID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfPaging3gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Paging3G'][0]['src_dir'] + os.sep + dirdate
	extract_to_dir = curdir+os.sep+ume+os.sep+'Trf_Thp_Paging_Avail_Pyld'+os.sep+'3G'

	df_result = pd.DataFrame()

	if ume == "UME_SUL" or ume == "UME_KAL":
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
				zipFileName = config_data['SRC_UME']['Paging3G'][0]['prefix_fname_sul']
			else :
				zipFileName = config_data['SRC_UME']['Paging3G'][0]['prefix_fname_kal']
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
			df_res = pd.read_csv(extract_to_dir+os.sep+file)
			df_res['GRANULARITY'] = 900
			df_res['tech'] = '3G'
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)
			# remove comma df paging columns
			df_res['Total number of cell paging requests of type 1'] = df_res['Total number of cell paging requests of type 1'].str.replace(',','').astype(float)
			# print(df_res['COLLECTTIME'])
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SubnetWork ID'
					,'NodeB ID'
					,'Cell ID'
					,'OSS'
					,'tech'
					,'Total number of cell paging requests of type 1'
				]
			]
			df_result.rename(columns={
				'SubnetWork ID' : 'CONTROLLERID'
				,'NodeB ID' : 'SITEID'
				,'Cell ID' : 'CELLID'
				,'Total number of cell paging requests of type 1' : 'paging'
			},inplace=True)
	else :
		print('Enter either UME_SUL or UME_KAL')

	return df_result

def joining_df_2g3gEms(ems):
	df_paging_2g = setDfPaging2gEms(ems)
	df_paging_3g = setDfPaging3gEms(ems)

	
	df_result = pd.concat([df_paging_2g,df_paging_3g])

	return df_result

def joining_df_2g3gUme(ume):
	df_paging_2g = setDfPaging2gUme(ume)
	# df_paging_3g = setDfPaging3gUme(ume) # no more 3G 

	# df_result = pd.concat([df_paging_2g,df_paging_3g])
	df_result = df_paging_2g

	return df_result

def parsing_paging():
	# no more EMS
	# df_ems5 = joining_df_2g3gEms("EMS5")
	# df_ems6 = joining_df_2g3gEms("EMS6")
	# df_ems7 = joining_df_2g3gEms("EMS7")
	df_ume_sul = joining_df_2g3gUme("UME_SUL")
	df_ume_puma = joining_df_2g3gUme("UME_PUMA")
	df_ume_kal = joining_df_2g3gUme("UME_KAL")

	df_result = pd.concat([df_ume_puma,df_ume_sul,df_ume_kal])
	df_result['primKey'] = df_result['CONTROLLERID'].astype(str)+df_result['SITEID'].astype(str)+df_result['CELLID'].astype(str)
	return df_result

def counting_paging(item):
	curdir = setCurDir()
	# print(curdir)
	df_paging = parsing_paging()
	df_paging['sitePrimKey'] = df_paging['CONTROLLERID'].astype(str)+df_paging['SITEID'].astype(str)
	df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
	df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(str)+df_data_poi['SITE_NUM'].astype(str)
	# return df_data_poi
	df_merge = df_paging.merge(df_data_poi,on='sitePrimKey',how='inner')
	if item == "poi_name" :
		df_pivot = np.round(pd.pivot_table(df_merge,values=['paging'],index=['POI_NAME','POI_LONGITUDE','POI_LATITUDE'],aggfunc=np.sum),2)
	else :
		df_pivot = np.round(pd.pivot_table(df_merge,values=['paging'],index=['NSA'],aggfunc=np.sum),2)
	df_result = df_pivot.reset_index()
	return df_result

# df = counting_paging('nsa')
# print(df)
# ems = "EMS7"
# df_res = parsing_paging()
# print(df_res)
# export_to_csv(df_res,'Paging_'+ems+'.csv')
# export_to_csv(df_res,'all_paging.csv')