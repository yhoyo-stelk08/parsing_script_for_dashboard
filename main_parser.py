import pandas as pd
import os

from add_func import export_to_csv,setCurDir
from parsing_traffic import parsing_traffic
from parsing_availability import parsing_availability
from parsing_payload import parsing_payload
from parsing_thp import parsing_thp
from parsing_paging import parsing_paging
from parsing_cpu_load import parsing_cpu_load_2g3g,parsing_cpu_load_4g
from parsing_utilport import parsing_utilport

def main_parser(meas):
	df_meas = pd.DataFrame()
	if meas == "traffic" :
		df_meas = parsing_traffic()
	elif meas == "payload" :
		df_meas = parsing_payload()
	elif meas == "thp" :
		df_meas = parsing_thp()
	elif meas == "paging" :
		df_meas = parsing_paging()
	elif meas == "cpu_load_2g3g" :
		df_meas = parsing_cpu_load_2g3g()
	elif meas == "cpu_load_4g" :
		df_meas = parsing_cpu_load_4g()
	elif meas == "portutil" :
		df_meas = parsing_utilport()
	elif meas == "availability" :
		df_meas = parsing_availability()
	else :
		print('Measurement not exist')

	fname_res = 'all_'+meas+'.csv'
	export_to_csv(df_meas,fname_res)
	db_operation(meas,fname_res)

def db_operation(meas,fname_res):
	curdir = setCurDir()
	# delete first line
	os.system('sed -i -e \'1d\' '+curdir+os.sep+'csv_results'+os.sep+fname_res)
	# truncate table meas
	os.system('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"TRUNCATE all_'+meas+'\"')
	# upload file results into tables
	# print('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"LOAD DATA LOCAL INFILE \''+curdir+os.sep+'csv_results'+os.sep+fname_res+'\' INTO TABLE all_'+meas+' FIELDS TERMINATED BY \',\' ENCLOSED BY \'\\"\' LINES TERMINATED BY \'\\n\'\"')
	os.system('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"LOAD DATA LOCAL INFILE \''+curdir+os.sep+'csv_results'+os.sep+fname_res+'\' INTO TABLE all_'+meas+' FIELDS TERMINATED BY \',\' ENCLOSED BY \'\\"\' LINES TERMINATED BY \'\\n\'\"')
	print('Data '+meas+' already upload to table all_'+meas)

main_parser("traffic")
main_parser("payload")
main_parser("thp")
main_parser("paging")
main_parser("cpu_load_2g3g")
main_parser("cpu_load_4g")
main_parser("portutil")
main_parser("availability")