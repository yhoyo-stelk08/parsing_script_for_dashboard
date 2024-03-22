import os
import re
import pandas as pd
import numpy as np
import json
import zipfile

from datetime import datetime,timedelta
from add_func import export_to_csv,setCurDir,readConfigFile

def setDfPortUtil2g3gEms(ems):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	file_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+"2G"
	list_file = os.listdir(file_dir)

	for file in list_file :
		result = re.search('.+ARAPMACFLOWMO'+'.+',file)

		if result :
			# print(result.group(0))
			filename = result.group(0)
			df_res = pd.read_csv(file_dir+os.sep+filename)
			df_res['tech'] = '2G'
			df_res['OSS'] = ems
			df_res['max_tx_bitrate'] = df_res['C380260010']
			df_res['mean_tx_bitrate'] = df_res['C380260009']
			df_res['max_rx_bitrate'] = df_res['C380260008']
			df_res['mean_rx_bitrate'] = df_res['C380260007']
			df_res['phys_bw'] = df_res['C380260006']
			df_res['keyUtilPort'] = df_res['SUBNETWORK'].astype(str)+"-1/"+df_res['SUBSYSTEMID'].astype(str)+"/"+df_res['UNITID'].astype(str)+"/"+df_res['LOGETHPORTID'].astype(str)

			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SUBNETWORK'
					,'OSS'
					,'tech'
					,'max_tx_bitrate'
					,'mean_tx_bitrate'
					,'max_rx_bitrate'
					,'mean_rx_bitrate'
					,'phys_bw'
					,'keyUtilPort'
				]
			]
			df_result.rename(columns={
				'SUBNETWORK' : 'CONTROLLERID'
			},inplace=True)
			return df_result

def setDfDataPort():
	curdir = setCurDir()
	df_res = pd.read_csv(curdir+os.sep+"data_port.csv")
	df_res['keyUtilPort'] = df_res['ID-BOARDINFO/Port']
	return df_res

def setDfPortUtil2g3gUme(ume):
	curdir = setCurDir()
	config_data = readConfigFile()
	
	dirdate = (datetime.today() - timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d')
	delta_hour =  (datetime.today() - timedelta(hours=1,minutes=25))
	filedate = (delta_hour).strftime('%Y%m%d')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	filedatetime = filedate + qtime
	
	filedir = config_data['SRC_UME']['PortUtil2G3G'][0]['src_dir'] + os.sep + dirdate
	extract_to_dir = curdir+os.sep+ume+os.sep+'Util'+os.sep+'2G3G'

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
				zipFileName = config_data['SRC_UME']['PortUtil2G3G'][0]['prefix_fname_sul']
			elif ume == "UME_PUMA" :
				zipFileName = config_data['SRC_UME']['PortUtil2G3G'][0]['prefix_fname_puma']
			else :
				zipFileName = config_data['SRC_UME']['PortUtil2G3G'][0]['prefix_fname_kal']
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
			# set tech
			df_res['tech'] = np.where(df_res['ManagedElementID'].str.contains('BSC'),'2G','3G')
			df_res['OSS'] = ume
			# set COLLECTTIME
			df_res['DATE'] = df_res['Begin Time'].str[0:10]
			df_res['TIME'] = df_res['Begin Time'].str[11:16]
			df_res['TANGGAL'] = (pd.to_datetime(df_res['DATE'],format='%Y-%m-%d')).dt.strftime('%Y%m%d')
			df_res['JAM'] = (pd.to_datetime(df_res['TIME'],format='%H:%M')).dt.strftime('%H%M')
			df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(str) + df_res['JAM'].astype(str)
			# remove comma from bit rate columns
			df_res['Physical bandwidth(Mbps)'] = df_res['Physical bandwidth(Mbps)'].str.replace(',','').astype(float)
			df_res['Max sending bit rate(bps)'] = df_res['Max sending bit rate(bps)'].str.replace(',','').astype(float)
			df_res['Mean sending bit rate(bps)'] = df_res['Mean sending bit rate(bps)'].str.replace(',','').astype(float)
			df_res['Max receiving bit rate(bps)'] = df_res['Max receiving bit rate(bps)'].str.replace(',','').astype(float)
			df_res['Mean receiving bit rate(bps)'] = df_res['Mean receiving bit rate(bps)'].str.replace(',','').astype(float)
			df_res['keyUtilPort'] = df_res['SubnentWork ID'].astype(str)+"-1/"+df_res['SubSystem ID'].astype(str)+"/"+df_res['Unit ID'].astype(str)+"/"+df_res['LogicalEthPort ID'].astype(str)

			df_result = df_res[
				[
					'COLLECTTIME'
					,'GRANULARITY'
					,'SubnentWork ID'
					,'OSS'
					,'tech'
					,'Max sending bit rate(bps)'
					,'Mean sending bit rate(bps)'
					,'Max receiving bit rate(bps)'
					,'Mean receiving bit rate(bps)'
					,'Physical bandwidth(Mbps)'
					,'keyUtilPort'
				]
			]
			df_result.rename(columns={
				'SubnentWork ID' : 'CONTROLLERID'
				,'Max sending bit rate(bps)' : 'max_tx_bitrate'
				,'Mean sending bit rate(bps)' : 'mean_tx_bitrate'
				,'Max receiving bit rate(bps)' : 'max_rx_bitrate'
				,'Mean receiving bit rate(bps)' : 'mean_rx_bitrate'
				,'Physical bandwidth(Mbps)' : 'phys_bw'
			},inplace=True)
	return df_result

def merging_data_util_port(oss):
	if oss[0:3] == "EMS" :
		# print(oss)
		df1 = setDfPortUtil2g3gEms(oss)
	else :
		# print(oss)
		df1 = setDfPortUtil2g3gUme(oss)
	df2 = setDfDataPort()
	
	df_merge = df1.merge(df2,on='keyUtilPort',how='inner')

	df_merge['mean_tx'] = (df_merge['mean_tx_bitrate'] * 100) / (df_merge['phys_bw'] * 1000000)
	df_merge['mean_rx'] = (df_merge['mean_rx_bitrate'] * 100) / (df_merge['phys_bw'] * 1000000)
	df_res = df_merge[
		[
			'COLLECTTIME'
			,'GRANULARITY'
			,'CONTROLLERID'
			,'OSS'
			,'tech'
			,'max_tx_bitrate'
			,'mean_tx_bitrate'
			,'max_rx_bitrate'
			,'mean_rx_bitrate'
			,'phys_bw'
			,'keyUtilPort'
			,'Remark'
			,'mean_tx'
			,'mean_rx'
		]
	]
	df_res.fillna('0',inplace=True)
	return df_res

def parsing_utilport():
	# no more EMS
	# df_ems5 = merging_data_util_port("EMS5")
	# df_ems6 = merging_data_util_port("EMS6")
	# df_ems7 = merging_data_util_port("EMS7")
	df_ume_sul = merging_data_util_port("UME_SUL")
	df_ume_puma = merging_data_util_port("UME_PUMA")
	df_ume_kal = merging_data_util_port("UME_KAL")
	df_result = pd.concat([df_ume_puma,df_ume_sul,df_ume_kal])
	# df_result = pd.concat([df_ems5,df_ems6,df_ems7])
	return df_result

def counting_utilport():
	df_utilport = parsing_utilport()
	df_pivot = np.round(pd.pivot_table(df_utilport,values=['mean_tx','mean_rx'],index=['Remark'],aggfunc=np.max),2)
	return df_pivot

# df = parsing_utilport()
# print(df)
# df = setDfPortUtil2g3gEms("EMS5")
# print(df)
# # df = setDfPortUtil2g3gEms("EMS7")
# # print(df)
# # df = merging_data_util_port("EMS5")
# # print(df)
# df = setDfPortUtil2g3gUme("UME_SUL")
# print(df)
# export_to_csv(df,'all_port_util.csv')
# # df = setDfPortUtil2g3gUme("UME_KAL")
# # print(df)