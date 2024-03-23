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

    filedir = config_data['SRC_UME']['Avail2G'][0]['src_dir'] + \
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

    # extract files from filedir
    for file in list_file:
        if ume == "UME_SUL":
            zipFileName = config_data['SRC_UME']['Avail2G'][0]['prefix_fname_sul']
        elif ume == "UME_KAL":
            zipFileName = config_data['SRC_UME']['Avail2G'][0]['prefix_fname_kal']
        else:
            zipFileName = config_data['SRC_UME']['Avail2G'][0]['prefix_fname_puma']
        # print(filedatetime)
        pattern = zipFileName+'_'+filedatetime
        # print(pattern)
        result = re.search(pattern+'.+', file)
        if result:
            print(result.group(0))
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
    filedir = config_data['SRC_UME']['Avail4G'][0]['src_dir'] + \
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
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_fdd_sul']
            elif ume == "UME_KAL":
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_fdd_kal']
            else:
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_fdd_puma']
        else:
            if ume == "UME_SUL":
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_tdd_sul']
            elif ume == "UME_KAL":
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_tdd_kal']
            else:
                zipFileName = config_data['SRC_UME']['Avail4G'][0]['prefix_fname_tdd_puma']
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


def setDfAvailability2gUme(ume):
    curdir = setCurDir()
    current_date = datetime.today().strftime('%Y-%m-%d')

    data_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'2G'

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # processing raw data
        # processing_raw_data_2g(ume)

        # set dataframe process
        for file in os.listdir(data_dir):
            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file)
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
            df_res['current_date'] = current_date
            # print(df_res['COLLECTTIME'])
            if ume == "UME_SUL":
                df_result = df_res[
                    [
                        'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TCH Availability (%) NFJ'
                    ]
                ]
                df_result.rename(columns={
                    'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TCH Availability (%) NFJ': 'availability'
                }, inplace=True)
            elif ume == "UME_KAL":
                df_result = df_res[
                    [
                        'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TCH Availability (%) NFJ_1638262476028'
                    ]
                ]
                df_result.rename(columns={
                    'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TCH Availability (%) NFJ_1638262476028': 'availability'
                }, inplace=True)
            elif ume == "UME_PUMA":
                df_result = df_res[
                    [
                        'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'SITE ID', 'BTS ID', 'OSS', 'tech', 'TCH Availability (%) NFJ'
                    ]
                ]
                df_result.rename(columns={
                    'SubnetWork ID': 'CONTROLLERID', 'SITE ID': 'SITEID', 'BTS ID': 'CELLID', 'TCH Availability (%) NFJ': 'availability'
                }, inplace=True)
            # return df_res
    else:
        print('Enter either UME_SUL or UME_KAL or UME_PUMA')

    df_result['availability'] = df_result['availability'].str.rstrip(
        '%').astype('float')/100
    return df_result


def setDfAvailability4gUme(ume, band):
    curdir = setCurDir()

    current_date = datetime.today().strftime('%Y-%m-%d')

    data_dir = curdir+os.sep+ume+os.sep + \
        'Trf_Thp_Paging_Avail_Pyld'+os.sep+'4G'+os.sep+band

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # processing raw data
        # processing_raw_data_4g(ume, band)

        # set dataframe process
        for file in os.listdir(data_dir):
            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file)
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
            df_res['current_date'] = current_date
            # print(df_res['COLLECTTIME'])
            if band == "FDD":
                if ume == "UME_SUL":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_FDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_FDD_SDRITBBU': 'availability'
                    }, inplace=True)
                elif ume == "UME_KAL":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_FDD'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_FDD': 'availability'
                    }, inplace=True)
                elif ume == "UME_PUMA":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_FDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_FDD_SDRITBBU': 'availability'
                    }, inplace=True)
                else:
                    print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
            elif band == "TDD":
                if ume == "UME_SUL":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_TDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_TDD_SDRITBBU': 'availability'
                    }, inplace=True)
                elif ume == "UME_PUMA":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_TDD_SDRITBBU'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_TDD_SDRITBBU': 'availability'
                    }, inplace=True)
                elif ume == "UME_KAL":
                    df_result = df_res[
                        [
                            'current_date', 'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'cellId', 'OSS', 'tech', 'Cell_Availability_TDD'
                        ]
                    ]
                    df_result.rename(columns={
                        'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'cellId': 'CELLID', 'Cell_Availability_TDD': 'availability'
                    }, inplace=True)
                else:
                    print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")
            else:
                print('Band value only FDD or TDD')

    else:
        print('Enter either UME_SUL or UME_KAL or UME_PUMA')

    df_result['availability'] = df_result['availability'].str.rstrip(
        '%').astype('float')/100
    return df_result


def joining_df_2g4gUme(ume):
    df_avail_2g = setDfAvailability2gUme(ume)
    df_avail_fdd = setDfAvailability4gUme(ume, "FDD")
    df_avail_tdd = setDfAvailability4gUme(ume, "TDD")

    df_result = pd.concat([df_avail_2g, df_avail_fdd, df_avail_tdd])
    return df_result


def counting_availability(item):
    df_data_poi = pd.read_csv('data_poi_cell.csv')

    df_data_poi['CI'] = df_data_poi['CI'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_data_poi['primKey'] = df_data_poi['CONTROLLER_NUM'].astype(
        str)+df_data_poi['SITE_NUM'].astype(str)+df_data_poi['CI'].astype(str)

    df_availability = parsing_availability()

    df_availability['SITEID'].fillna(0, inplace=True)
    df_availability['CELLID'].fillna(0, inplace=True)

    df_availability['SITEID'] = df_availability['SITEID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))
    df_availability['CELLID'] = df_availability['CELLID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_availability['primKey'] = df_availability['CONTROLLERID'].astype(
        str)+df_availability['SITEID'].astype(str)+df_availability['CELLID'].astype(str)

    df_merge = df_availability.merge(df_data_poi, on='primKey', how='inner')
    if item == "poi_name":
        df_pivot = np.round(pd.pivot_table(df_merge, values='availability', index=[
            'POI_NAME', 'POI_LONGITUDE', 'POI_LATITUDE'], aggfunc=np.mean), 2)
    else:
        df_pivot = pd.pivot_table(df_merge, values='availability', index=[
            'NSA'], aggfunc=np.mean)
    df_result = df_pivot.reset_index()
    df_result['percent_avail'] = (
        np.round(df_result['availability'] * 100, 0)).astype(str)+'%'
    return df_result


def parsing_availability():
    curdir = setCurDir()
    df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv', thousands=',')

    df_data_poi['CI'] = df_data_poi['CI'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_data_poi['primKey'] = df_data_poi['CONTROLLER_NUM'].astype(
        str)+df_data_poi['SITE_NUM'].astype(str)+df_data_poi['CI'].astype(str)


    df_ume_sul = joining_df_2g4gUme("UME_SUL")
    df_ume_kal = joining_df_2g4gUme("UME_KAL")
    df_ume_puma = joining_df_2g4gUme("UME_PUMA")
    df_concat = pd.concat([df_ume_sul, df_ume_kal, df_ume_puma])

    df_concat['SITEID'].fillna(0, inplace=True)
    df_concat['CELLID'].fillna(0, inplace=True)

    df_concat['SITEID'] = df_concat['SITEID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))
    df_concat['CELLID'] = df_concat['CELLID'].apply(
        lambda x: convert_site_cell(x, 'Linux'))

    df_concat['primKey'] = df_concat['CONTROLLERID'].astype(
        str)+df_concat['SITEID'].astype(str)+df_concat['CELLID'].astype(str)
    df_merge = df_concat.merge(df_data_poi, on='primKey', how='inner')
    df_result = df_merge[
        [
            'current_date', 'COLLECTTIME', 'GRANULARITY', 'CONTROLLERID', 'SITEID', 'CELLID', 'OSS', 'tech', 'REGION', 'availability'
        ]
    ]
    return df_result


# df_res = setDfAvailability4gUme('UME_PUMA', 'FDD')
df_res = parsing_availability()
print(df_res)
