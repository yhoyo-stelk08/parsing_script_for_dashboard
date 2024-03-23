import pandas as pd
import os

from add_func import export_to_csv, setCurDir
from parsing_traffic import counting_traffic
from parsing_availability import counting_availability
from parsing_payload import counting_payload
from parsing_thp import counting_throughput
from parsing_paging import counting_paging


def sub_parser(meas):
    df_meas = pd.DataFrame()
    if meas == "traffic_per_poi_name":
        df_meas = counting_traffic('poi_name')
    elif meas == "traffic_per_nsa":
        df_meas = counting_traffic('nsa')
    elif meas == "traffic_per_poi_category":
        df_meas = counting_traffic('poi_category')
    elif meas == "payload_per_poi_name":
        df_meas = counting_payload('poi_name')
    elif meas == "payload_per_poi_location":
        df_meas = counting_payload('poi_location')
    elif meas == "payload_per_nsa":
        df_meas = counting_payload("nsa")
    elif meas == "thp_per_poi_name":
        df_meas = counting_throughput('poi_name')
    elif meas == "thp_per_nsa":
        df_meas = counting_throughput('nsa')
    elif meas == "availability_per_poi_name":
        df_meas = counting_availability('poi_name')
    elif meas == "availability_per_nsa":
        df_meas = counting_availability('nsa')
    elif meas == "paging_per_poi_name":
        df_meas = counting_paging('poi_name')
    elif meas == "paging_per_nsa":
        df_meas = counting_paging('nsa')
    else:
        print('Measurement not exist')

    fname_res = 'all_'+meas+'.csv'
    export_to_csv(df_meas, fname_res)
    db_operation(meas, fname_res)
    # return df_meas


def db_operation(meas, fname_res):
    curdir = setCurDir()
    # delete first line
    os.system('sed -i -e \'1d\' '+curdir+os.sep+'csv_results'+os.sep+fname_res)
    # truncate table meas
    # os.system('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"TRUNCATE all_'+meas+'\"')
    # upload file results into tables
    # print('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"LOAD DATA LOCAL INFILE \''+curdir+os.sep+'csv_results'+os.sep+fname_res+'\' INTO TABLE all_'+meas+' FIELDS TERMINATED BY \',\' ENCLOSED BY \'\\"\' LINES TERMINATED BY \'\\n\'\"')
    os.system('/opt/lampp/bin/mysql -u rafi2020 -prafi20201q2w3e -D rafi2020 -e \"LOAD DATA LOCAL INFILE \''+curdir+os.sep+'csv_results' +
              os.sep+fname_res+'\' INTO TABLE all_'+meas+' FIELDS TERMINATED BY \',\' ENCLOSED BY \'\\"\' LINES TERMINATED BY \'\\n\'\"')
    print('Data '+meas+' already upload to table all_'+meas)


sub_parser("payload_per_poi_name")
sub_parser("payload_per_poi_location")
sub_parser("payload_per_nsa")
sub_parser("traffic_per_poi_name")
sub_parser("traffic_per_nsa")
sub_parser("traffic_per_poi_category")
sub_parser("thp_per_poi_name")
sub_parser("thp_per_nsa")
sub_parser("availability_per_poi_name")
sub_parser("availability_per_nsa")
sub_parser("paging_per_poi_name")
sub_parser("paging_per_nsa")
