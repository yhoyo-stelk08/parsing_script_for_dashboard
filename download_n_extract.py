import os
import json
import re
import tarfile
import shutil

from add_func import setCurDir,readConfigFile
from datetime import datetime,time,timedelta
from ftplib import FTP



def download_raw_files_ems(ems,tech):
	curdir = setCurDir()
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
		filedate = (datetime.today() - timedelta(minutes=0)).strftime('%Y%m%d')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
		filedate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%Y%m%d')
	
	if ems == "EMS7"  or ems == "EMS6":
		delta_hour =  (datetime.today() - timedelta(hours=1,minutes=30))
	else :
		delta_hour =  (datetime.today() - timedelta(minutes=30))
	yestdate = (datetime.today() - timedelta(days=1)).strftime('%d%b%Y')
	last_quarter_minute = 15*(delta_hour.minute//15)
	qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
	print(curdate+' '+qtime)

	config_data = readConfigFile()
	ftp_ip_address = config_data['NBI_EMS'][ems][0]["ip_address"]
	ftp_port = config_data['NBI_EMS'][ems][0]["port"]
	ftp_user = config_data['NBI_EMS'][ems][0]["username"]
	ftp_pass = config_data['NBI_EMS'][ems][0]["password"]

	# remove yesterday RAW files
	try :
		shutil.rmtree(curdir+os.sep+ems+os.sep+"RAW"+os.sep+yestdate)
	except :
		pass

	# remove all files under folder RAW 
	try :
		shutil.rmtree(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech)
	except :
		pass
	# make new folder current date under directory RAW
	try :
		os.mkdir(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate)
		# os.mkdir(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech)
	except FileExistsError :
		print('Folder'+curdate+'already created')
	# make new folder tech under directory current date
	try :
		# os.mkdir(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate)
		os.mkdir(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech)
	except FileExistsError :
		print('Folder'+tech+'already created')
	# change to current date + tech folder
	os.chdir(curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech)

	# connect and login to ftp
	ftp = FTP()
	ftp.connect(ftp_ip_address,ftp_port)
	ftp.login(ftp_user,ftp_pass)
	# change to files directory
	if tech == "2G" :
		ftp.cwd("/ppus/minos.ppu/minos-naf.pmu/db/GSMV3/PM")
	elif tech == "3G" :
		ftp.cwd("/ppus/minos.ppu/minos-naf.pmu/db/WCDMA/PM")
	else :
		ftp.cwd("/ppus/minos.ppu/minos-naf.pmu/db/LTE/PM")

	# Listing file name under files directory
	list_file = ftp.nlst()
	# downloading files
	for file in list_file :
		# print(file)
		if tech == "4G" :
			result = re.search('.+TELKOMSEL2_'+filedate+'_'+qtime+'.+',file)
		else :
			result = re.search('.+'+curdate+'_'+qtime+'-.+',file)

		if result :
			# print(result.group(0))
			print("Downloading "+tech+" file "+result.group(0)+" for "+ems)
			filename = result.group(0)
			with open(filename,"wb") as f :
				ftp.retrbinary(f"RETR {filename}",f.write)
				print("File "+result.group(0) + " for " + ems +" Download Done")
				f.close()
	ftp.quit()

def extract_raw_files_4g_ems(ems,tech,band):
	curdir = setCurDir()
	# curdate = datetime.today().strftime('%d%b%Y')
	if ems == "EMS5" :
		curdate = (datetime.today() - timedelta(minutes=0)).strftime('%d%b%Y')
		filedate = (datetime.today() - timedelta(minutes=0)).strftime('%Y%m%d')
	else :
		curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
		filedate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%Y%m%d')
	# curdate = (datetime.today() - timedelta(hours=1,minutes=30)).strftime('%d%b%Y')
	# yestdate = (datetime.today() - timedelta(days=1)).strftime('%d%b%Y')
	
	extract_from_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech
	extract_to_dir = curdir+os.sep+ems+os.sep+"RAW"+os.sep+curdate+os.sep+tech+os.sep+band
	
	# remove yesterday RAW files
	try :
		shutil.rmtree(curdir+os.sep+ems+os.sep+"RAW"+os.sep+yestdate)
	except :
		pass
	# change directory to extract_from_dir
	os.chdir(extract_from_dir)
	list_file = os.listdir()
	
	# make new folder current date under directory RAW
	try :
		os.mkdir(extract_to_dir)
	except FileExistsError :
		pass

	# delete all files under dir band
	for file in os.scandir(extract_to_dir) :
		shutil.rmtree(file)
	

	# extract process
	for file in list_file :
		result = re.search('.+'+band+'*.*',file)
		if result :
			print('Extracting file '+result.group(0))
			filename = result.group(0)
			# open file
			file_tar = tarfile.open(filename)
			file_tar.extractall(extract_to_dir)
			file_tar.close()
			print("File "+filename+" telah di extract ke "+extract_to_dir)



# Download 2G 
download_raw_files_ems("EMS5","2G")
download_raw_files_ems("EMS6","2G")
download_raw_files_ems("EMS7","2G")

# Download 3G 
download_raw_files_ems("EMS5","3G")
download_raw_files_ems("EMS6","3G")
download_raw_files_ems("EMS7","3G")

# Download 4G 
download_raw_files_ems("EMS5","4G")
download_raw_files_ems("EMS6","4G")
download_raw_files_ems("EMS7","4G")

# Extract file 4G 
extract_raw_files_4g_ems("EMS5","4G","FDD")
extract_raw_files_4g_ems("EMS5","4G","TDD")
extract_raw_files_4g_ems("EMS6","4G","FDD")
extract_raw_files_4g_ems("EMS6","4G","TDD")
extract_raw_files_4g_ems("EMS7","4G","FDD")
extract_raw_files_4g_ems("EMS7","4G","TDD")