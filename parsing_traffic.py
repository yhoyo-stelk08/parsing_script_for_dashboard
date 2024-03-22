import os
import re
import pandas as pd
import numpy as np
import json
import zipfile

from datetime import datetime,timedelta
from add_func import export_to_csv,setCurDir,readConfigFile

def setDfTraffic2gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=0)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"2G"
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+ABSCRRAVAILMEAS'+'.+',file)

		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename)
			df_res['tech'] = '2G'
			df_res['OSS'] = ems
			df_res['tch_traffic_erl'] = (df_res['C901080016']+df_res['C901080017']+df_res['C901080024']+df_res['C901080025']+df_res['C901080026']+df_res['C901080047'])/3600
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'IBSCMEID'
					,'SITEID'
					,'BTSID'
					,'OSS'
					,'tech'
					,'tch_traffic_erl'
				]
			]
			df_result.rename(columns={
				'IBSCMEID' : 'CONTROLLERID'
				,'BTSID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfTraffic2gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Traffic2G'][0]['src_dir'] + os.sep + dirdate
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
				zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_kal']
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
					,'TCH total traffic number(erl)'
				]
			]
			df_result.rename(columns={
				'SubnetWork ID' : 'CONTROLLERID'
				,'SITE ID' : 'SITEID'
				,'BTS ID' : 'CELLID'
				,'TCH total traffic number(erl)' : 'tch_traffic_erl'
			},inplace=True)
			# return df_res
	else :
		print('Enter either UME_SUL or UME_KAL or UME_PUMA')
	
	return df_result
	# print(filedir)
	# print(qtime)

def setDfTraffic3gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=0)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"3G"
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+AR9CELLHOLDTIME'+'.+',file)

		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename)
			df_res['tech'] = '3G'
			df_res['OSS'] = ems
			df_res['tch_traffic_erl'] = (df_res['C310040001']+df_res['C310040002']+df_res['C310040003']+df_res['C310040004']+df_res['C310040005']+df_res['C310040006']+df_res['C310040007']+df_res['C310040008']+df_res['C310040009']+df_res['C310040010']+df_res['C310040011']+df_res['C310040012']+df_res['C310040013']+df_res['C310040014']+df_res['C310040015']+df_res['C310040016']+df_res['C310040017']+df_res['C310040018']+df_res['C310040019']+df_res['C310040020']+df_res['C310040021']+df_res['C310040022']+df_res['C310040075']+df_res['C310040076']+df_res['C310040077']+df_res['C310040078']+df_res['C310040079']+df_res['C310040080']+df_res['C310040081']+df_res['C310040082']+df_res['C310040083']+df_res['C310040084']+df_res['C310040085']+df_res['C310040086']+df_res['C310040087']+df_res['C310040088']+df_res['C310040089']+df_res['C310040090']+df_res['C310040091']+df_res['C310040092']+df_res['C310040093']+df_res['C310040094']+df_res['C310040095']+df_res['C310040096'])/3600
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'RNCID'
					,'NODEBID'
					,'OBJECTID'
					,'OSS'
					,'tech'
					,'tch_traffic_erl'
				]
			]
			df_result.rename(columns={
				'RNCID' : 'CONTROLLERID'
				,'NODEBID' : 'SITEID'
				,'OBJECTID' : 'CELLID'
			},inplace=True)
			return df_result

def setDfTraffic3gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['Traffic3G'][0]['src_dir'] + os.sep + dirdate
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
				zipFileName = config_data['SRC_UME']['Traffic3G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['Traffic3G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['Traffic3G'][0]['prefix_fname_kal']
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
			df_res = pd.read_csv(extract_to_dir+os.sep+file,thousands=",")
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
			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SubnetWork ID'
					,'NodeB ID'
					,'Cell ID'
					,'OSS'
					,'tech'
					,'3G Traffic CS (Erlang) NFJ_1584858004616-0-0'
				]
			]
			df_result.rename(columns={
				'SubnetWork ID' : 'CONTROLLERID'
				,'NodeB ID' : 'SITEID'
				,'Cell ID' : 'CELLID'
				,'3G Traffic CS (Erlang) NFJ_1584858004616-0-0' : 'tch_traffic_erl'
			},inplace=True)
	else :
		print('Enter either UME_SUL or UME_KAL')

	return df_result

def joining_df_2g3gEms(ems):
	df_traff_2g_ems = setDfTraffic2gEms(ems)
	df_traff_3g_ems = setDfTraffic3gEms(ems)

	
	df_result = pd.concat([df_traff_2g_ems,df_traff_3g_ems])

	return df_result

def joining_df_2g3gUme(ume):
	df_traff_2g_ume = setDfTraffic2gUme(ume)
	df_traff_3g_ume = setDfTraffic3gUme(ume)

	
	df_result = pd.concat([df_traff_2g_ume,df_traff_3g_ume])

	return df_result

def parsing_traffic():
	# no EMS anymore
	# df_ems5 = joining_df_2g3gEms("EMS5")
	# df_ems6 = joining_df_2g3gEms("EMS6")
	# df_ems7 = joining_df_2g3gEms("EMS7")
	df_ume_sul = joining_df_2g3gUme("UME_SUL")
	df_ume_puma = joining_df_2g3gUme("UME_PUMA")
	df_ume_kal = joining_df_2g3gUme("UME_KAL")

	# df_result = pd.concat([df_ems5,df_ems6,df_ems7,df_ume_sul,df_ume_kal])
	df_result = pd.concat([df_ume_sul,df_ume_puma,df_ume_kal])
	df_result['primKey'] = df_result['CONTROLLERID'].astype(str)+df_result['SITEID'].astype(str)+df_result['CELLID'].astype(str)
	return df_result

def counting_traffic(item):
	curdir = setCurDir()
	# delta_hour =  (datetime.today() - timedelta(hours=1,minutes=45))
	delta_hour =  (datetime.today() - timedelta(minutes=0))
	curdate = (delta_hour).strftime('%Y-%m-%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
	cur_datetime = curdate +' '+ qtime

	df_traffic = parsing_traffic()
	df_traffic['datetime_id'] = cur_datetime
	df_traffic['sitePrimKey'] = df_traffic['CONTROLLERID'].astype(str)+df_traffic['SITEID'].astype(str)
	df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
	df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(str)+df_data_poi['SITE_NUM'].astype(str)
	# return df_data_poi
	df_merge = df_traffic.merge(df_data_poi,on='sitePrimKey',how='inner')
	if item == "poi_name" :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','POI_NAME','POI_LONGITUDE','POI_LATITUDE'],aggfunc=np.sum),2)
	elif item == "poi_category" :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','POI_CATEGORY'],aggfunc=np.sum),2)
	else :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','NSA'],aggfunc=np.sum),2)
	df_result = df_pivot.reset_index()
	return df_result

# df_res = parsing_traffic()
# print(df_res)


def testing(item):
	curdir = setCurDir()
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=45))
	curdate = (delta_hour).strftime('%Y-%m-%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
	cur_datetime = curdate +' '+ qtime

	df_traffic = parsing_traffic()
	df_traffic['datetime_id'] = cur_datetime
	df_traffic['sitePrimKey'] = df_traffic['CONTROLLERID'].astype(str)+df_traffic['SITEID'].astype(str)
	df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
	df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(str)+df_data_poi['SITE_NUM'].astype(str)
	# return df_data_poi
	df_merge = df_traffic.merge(df_data_poi,on='sitePrimKey',how='inner')
	if item == "poi_name" :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','POI_NAME','POI_LONGITUDE','POI_LATITUDE'],aggfunc=np.sum),2)
	elif item == "poi_category" :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','POI_CATEGORY'],aggfunc=np.sum),2)
	else :
		df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','NSA'],aggfunc=np.sum),2)
	df_result = df_pivot.reset_index()
	return df_result


# df = testing('poi_name')
# print(df)
# def test_func():
# 	curdir = setCurDir()
# 	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=45))
# 	curdate = (delta_hour).strftime('%Y-%m-%d')
# 	last_quarter_minute = 15*(delta_hour.minute//15)
# 	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
# 	cur_datetime = curdate +' '+ qtime

# 	df_traffic = parsing_traffic()
# 	df_traffic['datetime_id'] = cur_datetime
# 	df_traffic['sitePrimKey'] = df_traffic['CONTROLLERID'].astype(str)+df_traffic['SITEID'].astype(str)
# 	df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
# 	df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(str)+df_data_poi['SITE_NUM'].astype(str)
# 	# return df_data_poi
# 	df_merge = df_traffic.merge(df_data_poi,on='sitePrimKey',how='inner')
# 	df_pivot = np.round(pd.pivot_table(df_merge,values='tch_traffic_erl',index=['datetime_id','NSA'],aggfunc=np.sum),2)
# 	df_result = df_pivot.reset_index()
# 	return df_result


# # test_func()
# df_res = test_func()
# print(df_res)
# export_to_csv(df_res,'tch_traffic_all.csv')