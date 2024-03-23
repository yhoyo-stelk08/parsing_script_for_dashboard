import os
import re
import pandas as pd
import numpy as np
import zipfile

from datetime import datetime, timedelta
from add_func import export_to_csv, setCurDir, readConfigFile, convert_site_cell


def processing_raw_data_2g(ume):
    curdir = setCurDir()
    config_data = readConfigFile()
    dirdate = (datetime.today() - timedelta(hours=1,
                minutes=25)).strftime('%Y-%m-%d')
    delta_hour = (datetime.today() - timedelta(hours=1, minutes=25))
    filedate = (delta_hour).strftime('%Y%m%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
    filedatetime = filedate + qtime

    filedir = config_data['SRC_UME']['Payload2G'][0]['src_dir'] + \
        os.sep + dirdate
    extract_to_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

    # change directory to filedir directory
    os.chdir(filedir)

    # list all files inside filedir directory
    list_file = os.listdir(filedir)

    # delete all files under dir band
    for file in os.listdir(extract_to_dir):
        # shutil.rmtree(file)
        os.remove(extract_to_dir+os.sep+file)
        # print(file)

    # exracting raw files process
    for file in list_file:
        if ume == "UME_SUL":
            zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_sul']
        elif ume == "UME_PUMA":
            zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_puma']
        elif ume == "UME_KAL":
            zipFileName = config_data['SRC_UME']['Payload2G'][0]['prefix_fname_kal']
        else:
            print("No files in listed files.")

        # print(filedatetime)
        pattern = zipFileName+'_'+filedatetime
        # print(pattern)
        result = re.search(pattern+'.+', file)
        if result:
            print(result.group(0))
            filename = result.group(0)
            # extract files from filedir into extract_to_dir
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


def processing_raw_data_4g(ume, band):
    curdir = setCurDir()
    config_data = readConfigFile()
    dirdate = (datetime.today() - timedelta(hours=1,
               minutes=25)).strftime('%Y-%m-%d')
    delta_hour = (datetime.today() - timedelta(hours=1, minutes=25))
    filedate = (delta_hour).strftime('%Y%m%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
    filedatetime = filedate + qtime

    filedir = config_data['SRC_UME']['Payload4G'][0]['src_dir'] + \
        os.sep + dirdate
    extract_to_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'4G'+os.sep+band

    # change directory to filedir directory
    os.chdir(filedir)

    # list all files inside filedir directory
    list_file = os.listdir(filedir)

    # delete all files under dir band
    for file in os.listdir(extract_to_dir):
        # shutil.rmtree(file)
        os.remove(extract_to_dir+os.sep+file)
        # print(file)

    # extract files from filedir
    for file in list_file:
        if band == "FDD":
            if ume == "UME_SUL":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_sul']
            elif ume == "UME_PUMA":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_puma']
            elif ume == "UME_KAL":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_fdd_kal']
        else:
            if ume == "UME_SUL":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_sul']
            elif ume == "UME_PUMA":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_puma']
            elif ume == "UME_KAL":
                zipFileName = config_data['SRC_UME']['Payload4G'][0]['prefix_fname_tdd_kal']
        # print(filedatetime)
        pattern = zipFileName+'_'+filedatetime
        # print(pattern)
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


def setDfPayload2gUme(ume):
    curdir = setCurDir()

    # setting path for file to extract
    data_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # method for preparing raw data
        processing_raw_data_2g(ume)

        # set dataframe process
        for file in os.listdir(data_dir):

            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file, thousands=',')
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
            if ume == "UME_SUL" or ume == "UME_PUMA":
                df_result = df_res[
                    [
                        'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TOTAL PAYLOAD (EDGE+GPRS)'
                    ]
                ]
                df_result.rename(columns={
                    'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TOTAL PAYLOAD (EDGE+GPRS)': 'payload_mbyte'
                }, inplace=True)
            elif ume == "UME_KAL":
                df_result = df_res[
                    [
                        'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TOTAL PAYLOAD (EDGE+GPRS)'
                    ]
                ]
                df_result.rename(columns={
                    'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TOTAL PAYLOAD (EDGE+GPRS)': 'payload_mbyte'
                }, inplace=True)
            # return df_res
    else:
        print('Enter either UME_SUL or UME_KAL or UME_PUMA')

    # df_result['availability'] = df_result['availability'].str.rstrip('%').astype('float')/100
    return df_result


def setDfPayload4gUme(ume, band):
    curdir = setCurDir()

    data_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'4G'+os.sep+band

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # method for preparing raw data
        processing_raw_data_4g(ume, band)

        # set dataframe process
        for file in os.listdir(data_dir):
            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file, thousands=",")
            df_res['GRANULARITY'] = 900
            df_res['tech'] = '4G ' + band
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
            if band == "FDD":
                if ume == "UME_SUL":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_FDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_FDD_SDRITBBU': 'payload_mbyte'
                    }, inplace=True)
                elif ume == "UME_PUMA":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_FDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_FDD_SDRITBBU': 'payload_mbyte'
                    }, inplace=True)
                elif ume == "UME_KAL":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_FDD'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_FDD': 'payload_mbyte'
                    }, inplace=True)
                else:
                    print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
            elif band == "TDD":
                if ume == "UME_SUL":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_TDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_TDD_SDRITBBU': 'payload_mbyte'
                    }, inplace=True)
                elif ume == "UME_PUMA":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_TDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_TDD_SDRITBBU': 'payload_mbyte'
                    }, inplace=True)
                elif ume == "UME_KAL":
                    df_result = df_res[
                        [
                            'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', '4G_Payload_TDD'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', '4G_Payload_TDD': 'payload_mbyte'
                    }, inplace=True)
                else:
                    print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
            else:
                print('Band value only FDD or TDD')

    else:
        print('Enter either UME_SUL or UME_KAL or UME_PUMA')

    # df_result['payload_mbyte'] = df_result['payload_mbyte'].str.rstrip('%').astype('float')/100
    return df_result


def joining_df_2g4gUme(ume):
    df_pyld_2g = setDfPayload2gUme(ume)
    df_pyld_fdd = setDfPayload4gUme(ume, "FDD")
    df_pyld_tdd = setDfPayload4gUme(ume, "TDD")

    df_result = pd.concat([df_pyld_2g, df_pyld_fdd, df_pyld_tdd])

    return df_result


def parsing_payload():
    df_ume_sul = joining_df_2g4gUme("UME_SUL")
    df_ume_kal = joining_df_2g4gUme("UME_KAL")
    df_ume_puma = joining_df_2g4gUme("UME_PUMA")
    # df_result = df_ems5
    df_result = pd.concat([df_ume_puma, df_ume_sul, df_ume_kal])

    df_result['SITEID'].fillna(0, inplace=True)
    df_result['CELLID'].fillna(0, inplace=True)

    df_result['SITEID'] = df_result['SITEID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))
    df_result['CELLID'] = df_result['CELLID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_result['primKey'] = df_result['CONTROLLERID'].astype(
        str)+df_result['SITEID'].astype(str)+df_result['CELLID'].astype(str)

    return df_result


def counting_payload(item):
    curdir = setCurDir()
    delta_hour = (datetime.today() - timedelta(minutes=0))
    curdate = (delta_hour).strftime('%Y-%m-%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H:%M')
    cur_datetime = curdate + ' ' + qtime

    df_payload = parsing_payload()
    df_payload['datetime_id'] = cur_datetime

    df_payload.drop_duplicates(subset='primKey', keep='first', inplace=True)

    df_data_poi = pd.read_csv(curdir + os.sep + 'data_poi_site.csv')

    df_data_poi['CI'] = df_data_poi['CI'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_data_poi['primKey'] = df_data_poi['CONTROLLER_NUM'].astype(
        str) + df_data_poi['SITE_NUM'].astype(str) + df_data_poi['CI'].astype(str)

    # Drop duplicates in df_data_poi based on primKey
    df_data_poi.drop_duplicates(subset='primKey', keep='first', inplace=True)

    df_merge = df_payload.merge(df_data_poi, on='primKey', how='inner')
    df_merge['payload_gbyte'] = np.round((df_merge['payload_mbyte'])/1000, 2)

    if item == "poi_name":
        df_pivot = pd.pivot_table(df_merge,
                                  values=['payload_gbyte',
                                          'POI_LONGITUDE',
                                          'POI_LATITUDE'],
                                  index=['datetime_id', 'POI_NAME'],
                                  aggfunc={
                                      'payload_gbyte': np.sum,
                                      'POI_LONGITUDE': 'first',
                                      'POI_LATITUDE': 'first',
                                  }
                                  )
    elif item == "poi_location":
        df_pivot = pd.pivot_table(df_merge, values='payload_gbyte', index=[
                                  'datetime_id', 'POI_LOCATION'], aggfunc=np.sum)
    else:
        df_pivot = pd.pivot_table(df_merge, values='payload_gbyte', index=[
                                  'datetime_id', 'NSA'], aggfunc=np.sum)

    df_result = df_pivot.reset_index()
    return df_result
