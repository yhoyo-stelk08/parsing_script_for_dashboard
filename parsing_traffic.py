import os
import re
import pandas as pd
import numpy as np
import json
import zipfile

from datetime import datetime, timedelta
from add_func import export_to_csv, setCurDir, readConfigFile, convert_site_cell


def processing_raw_data(ume):
    curdir = setCurDir()
    config_data = readConfigFile()

    dirdate = (datetime.today() - timedelta(hours=1,
               minutes=25)).strftime('%Y-%m-%d')
    delta_hour = (datetime.today() - timedelta(hours=1, minutes=25))
    filedate = (delta_hour).strftime('%Y%m%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
    filedatetime = filedate + qtime

    filedir = config_data['SRC_UME']['Traffic2G'][0]['src_dir'] + \
        os.sep + dirdate
    extract_to_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

    # change directory to filedir directory
    os.chdir(filedir)
    # list all files inside filedir directory
    list_file = os.listdir(filedir)

    # delete all files under dir
    for file in os.listdir(extract_to_dir):
        # shutil.rmtree(file)
        os.remove(extract_to_dir+os.sep+file)
        # print(file)
    # extract files from filedir
    for file in list_file:
        if ume == "UME_SUL":
            zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_sul']
        elif ume == "UME_PUMA":
            zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_puma']
        else:
            zipFileName = config_data['SRC_UME']['Traffic2G'][0]['prefix_fname_kal']
        # print(filedatetime)
        pattern = zipFileName+'_'+filedatetime
        # print(filename)
        result = re.search(pattern+'.+', file)
        if result:
            # print(result.group(0))
            filename = result.group(0)
            print('Extracting file '+filename)
            with zipfile.ZipFile(filedir+os.sep+filename, 'r') as zip_ref:
                zip_ref.extractall(extract_to_dir+os.sep)
                print("File "+filename+" telah di extract ke "+extract_to_dir)

    # delete unneeded files
    for file in os.listdir(extract_to_dir):
        result = re.search('.+kpis.+', file)
        if result:
            # print(result.group(0))
            # delete file kpis in extract_to_dir
            kpis_files = result.group(0)
            os.remove(extract_to_dir+os.sep+kpis_files)


def setDfTraffic2gUme(ume):
    curdir = setCurDir()

    extract_to_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # processing raw data method
        # processing_raw_data(ume)

        # set dataframe process
        for file in os.listdir(extract_to_dir):
            # print(file)
            df_res = pd.read_csv(extract_to_dir+os.sep+file)
            df_res['GRANULARITY'] = 900
            df_res['tech'] = '2G'
            df_res['OSS'] = ume
            # set COLLECTTIME
            df_res['DATE'] = df_res['Begin Time'].str[0:10]
            df_res['TIME'] = df_res['Begin Time'].str[11:16]
            df_res['TANGGAL'] = (pd.to_datetime(
                df_res['DATE'], format='%Y-%m-%d')).dt.strftime('%Y%m%d')
            df_res['JAM'] = (pd.to_datetime(
                df_res['TIME'], format='%H:%M')).dt.strftime('%H%M')
            df_res['COLLECTTIME'] = df_res['TANGGAL'].astype(
                str) + df_res['JAM'].astype(str)
            # print(df_res['COLLECTTIME'])
            df_result = df_res[
                [
                    'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TCH total traffic number(erl)'
                ]
            ]
            df_result.rename(columns={
                'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TCH total traffic number(erl)': 'tch_traffic_erl'
            }, inplace=True)
            # return df_res
    else:
        print('Enter either UME_SUL or UME_KAL or UME_PUMA')

    return df_result


def parsing_traffic():
    df_ume_sul = setDfTraffic2gUme("UME_SUL")
    df_ume_puma = setDfTraffic2gUme("UME_PUMA")
    df_ume_kal = setDfTraffic2gUme("UME_KAL")

    df_result = pd.concat([df_ume_sul, df_ume_puma, df_ume_kal])
    df_result['primKey'] = df_result['CONTROLLERID'].astype(
        str)+df_result['SITEID'].astype(str)+df_result['CELLID'].astype(str)
    return df_result


def counting_traffic(item):
    curdir = setCurDir()
    # delta_hour =  (datetime.today() - timedelta(hours=1,minutes=45))
    delta_hour = (datetime.today() - timedelta(minutes=0))
    curdate = (delta_hour).strftime('%Y-%m-%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
    cur_datetime = curdate + ' ' + qtime

    df_traffic = parsing_traffic()
    df_traffic['datetime_id'] = cur_datetime
    df_traffic['sitePrimKey'] = df_traffic['CONTROLLERID'].astype(
        str)+df_traffic['SITEID'].astype(str)
    df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
    df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(
        str)+df_data_poi['SITE_NUM'].astype(str)
    # return df_data_poi
    df_merge = df_traffic.merge(df_data_poi, on='sitePrimKey', how='inner')
    if item == "poi_name":
        df_pivot = np.round(pd.pivot_table(df_merge, values='tch_traffic_erl', index=[
                            'datetime_id', 'POI_NAME', 'POI_LONGITUDE', 'POI_LATITUDE'], aggfunc=np.sum), 2)
    elif item == "poi_category":
        df_pivot = np.round(pd.pivot_table(df_merge, values='tch_traffic_erl', index=[
                            'datetime_id', 'POI_CATEGORY'], aggfunc=np.sum), 2)
    else:
        df_pivot = np.round(pd.pivot_table(df_merge, values='tch_traffic_erl', index=[
                            'datetime_id', 'NSA'], aggfunc=np.sum), 2)
    df_result = df_pivot.reset_index()
    return df_result

# df_res = parsing_traffic()
# print(df_res)
