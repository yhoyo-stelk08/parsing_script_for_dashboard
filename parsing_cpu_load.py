from concurrent.futures import process
import os
import re
import pandas as pd
import numpy as np
import zipfile

from datetime import datetime, timedelta
from add_func import export_to_csv, setCurDir, readConfigFile


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

    filedir = config_data['SRC_UME']['CpuLoad2G3G'][0]['src_dir'] + \
        os.sep + dirdate
    extract_to_dir = curdir+os.sep+ume+os.sep+'Cpu'+os.sep+'2G3G'

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
            zipFileName = config_data['SRC_UME']['CpuLoad2G3G'][0]['prefix_fname_sul']
        elif ume == "UME_PUMA":
            zipFileName = config_data['SRC_UME']['CpuLoad2G3G'][0]['prefix_fname_puma']
        else:
            zipFileName = config_data['SRC_UME']['CpuLoad2G3G'][0]['prefix_fname_kal']
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


def processing_raw_data_4g(ume):
    curdir = setCurDir()
    config_data = readConfigFile()

    dirdate = (datetime.today() - timedelta(hours=1,
               minutes=25)).strftime('%Y-%m-%d')
    delta_hour = (datetime.today() - timedelta(hours=1, minutes=25))
    filedate = (delta_hour).strftime('%Y%m%d')
    last_quarter_minute = 15*(delta_hour.minute//15)
    qtime = delta_hour.replace(minute=last_quarter_minute).strftime('%H%M')
    filedatetime = filedate + qtime

    filedir = config_data['SRC_UME']['CpuLoad4G'][0]['src_dir'] + \
        os.sep + dirdate
    extract_to_dir = curdir+os.sep+ume+os.sep+'Cpu'+os.sep+'4G'

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
            zipFileName = config_data['SRC_UME']['CpuLoad4G'][0]['prefix_fname_sul']
        elif ume == "UME_KAL":
            zipFileName = config_data['SRC_UME']['CpuLoad4G'][0]['prefix_fname_kal']
        elif ume == "UME_PUMA":
            zipFileName = config_data['SRC_UME']['CpuLoad4G'][0]['prefix_fname_puma']

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


def setDfCpuLoad2gUme(ume):
    curdir = setCurDir()

    data_dir = curdir+os.sep+ume+os.sep+'Cpu'+os.sep+'2G3G'

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # processing raw data
        # processing_raw_data_2g(ume)

        # set dataframe process
        for file in os.listdir(data_dir):
            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file)
            df_res['GRANULARITY'] = 900
            # set tech
            df_res['tech'] = np.where(
                df_res['SubnetWork Name'].str.contains('BSC'), '2G', '3G')
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
            df_result = df_res[
                [
                    'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'OSS', 'tech', 'Max ratio of CPU usage(%)', 'Mean ratio of CPU usage(%)'
                ]
            ]
            df_result.rename(columns={
                'SubnetWork ID': 'SUBNETWORK', 'Max ratio of CPU usage(%)': 'max_ratio_cpu', 'Mean ratio of CPU usage(%)': 'mean_ratio_cpu'
            }, inplace=True)

    df_result['max_ratio_cpu'] = df_result['max_ratio_cpu'].str.rstrip(
        '%').astype('float')/100
    df_result['mean_ratio_cpu'] = df_result['mean_ratio_cpu'].str.rstrip(
        '%').astype('float')/100
    return df_result


def setDfCpuLoad4gUme(ume):
    curdir = setCurDir()

    data_dir = curdir+os.sep+ume+os.sep+'Cpu'+os.sep+'4G'

    df_result = pd.DataFrame()

    if ume == "UME_SUL" or ume == "UME_KAL" or ume == "UME_PUMA":

        # processing raw data
        # processing_raw_data_4g(ume)

        # set dataframe process
        for file in os.listdir(data_dir):
            # print(file)
            df_res = pd.read_csv(data_dir+os.sep+file)
            df_res['GRANULARITY'] = 900
            df_res['tech'] = '4G '
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
                    'COLLECTTIME', 'GRANULARITY', 'SubnetWork ID', 'eNodeBId', 'OSS', 'tech', 'Peak CPU Utilization Rate of ENB CC Board(%)', 'Average CPU Utilization Rate of ENB CC Board(%)'
                ]
            ]

            df_result.rename(columns={
                'SubnetWork ID': 'CONTROLLERID', 'eNodeBId': 'SITEID', 'Peak CPU Utilization Rate of ENB CC Board(%)': 'max_ratio_cpu', 'Average CPU Utilization Rate of ENB CC Board(%)': 'mean_ratio_cpu'
            }, inplace=True)


    else:
        print("Ume Value Either UME_SUL or UME_KAL or UME_PUMA")

    df_result['max_ratio_cpu'] = df_result['max_ratio_cpu'].str.rstrip(
        '%').astype('float')/100
    df_result['mean_ratio_cpu'] = df_result['mean_ratio_cpu'].str.rstrip(
        '%').astype('float')/100
    return df_result


def parsing_cpu_load_2g():
    # No More EMS #
    # df_ems5 = setDfCpuLoad2g3gEms("EMS5")
    # df_ems6 = setDfCpuLoad2g3gEms("EMS6")
    # df_ems7 = setDfCpuLoad2g3gEms("EMS7")
    df_ume_sul = setDfCpuLoad2gUme("UME_SUL")
    df_ume_puma = setDfCpuLoad2gUme("UME_PUMA")
    df_ume_kal = setDfCpuLoad2gUme("UME_KAL")

    # df_result = pd.concat([df_ems5,df_ems6,df_ems7,df_ume_sul,df_ume_kal])
    df_result = pd.concat([df_ume_sul, df_ume_puma, df_ume_kal])
    return df_result


def parsing_cpu_load_4g():
    # No More EMS #
    # df_ems5 = joining_df_4g_ems("EMS5")
    # df_ems6 = joining_df_4g_ems("EMS6")
    # df_ems7 = joining_df_4g_ems("EMS7")
    df_ume_kal = setDfCpuLoad4gUme("UME_KAL")
    df_ume_sul = setDfCpuLoad4gUme("UME_SUL")
    df_ume_puma = setDfCpuLoad4gUme("UME_PUMA")

    # df_result = pd.concat([df_ems5,df_ems6,df_ems7,df_ume_kal,df_ume_sul])
    df_result = pd.concat([df_ume_kal, df_ume_puma, df_ume_sul])
    return df_result


def counting_cpu_load_2g():
    curdir = setCurDir()
    # print(curdir)
    df_cpload = parsing_cpu_load_2g()
    df_cpload['controllerPrimKey'] = df_cpload['SUBNETWORK'].astype(str)
    df_controller = pd.read_csv(curdir+os.sep+'controller_data.csv')
    df_controller['controllerPrimKey'] = df_controller['subnetwork_id'].astype(
        str)
    # return df_data_poi
    df_merge = df_cpload.merge(
        df_controller, on='controllerPrimKey', how='inner')
    df_pivot = np.round(pd.pivot_table(df_merge, values=['max_ratio_cpu', 'mean_ratio_cpu'], index=[
                        'subnetwork_id', 'subnetwork_name', 'oss', 'regional', 'tech_y'], aggfunc=np.max), 2)
    df_result = df_pivot.reset_index()
    return df_result


def counting_cpu_load_4g():
    curdir = setCurDir()
    # print(curdir)
    df_cpload = parsing_cpu_load_4g()
    df_cpload['sitePrimKey'] = df_cpload['CONTROLLERID'].astype(
        str)+df_cpload['SITEID'].astype(str)
    df_data_poi = pd.read_csv(curdir+os.sep+'data_poi_site.csv')
    df_data_poi['sitePrimKey'] = df_data_poi['CONTROLLER_NUM'].astype(
        str)+df_data_poi['SITE_NUM'].astype(str)
    # return df_data_poi
    df_merge = df_cpload.merge(df_data_poi, on='sitePrimKey', how='inner')
    df_pivot = np.round(pd.pivot_table(df_merge, values=[
                        'max_ratio_cpu', 'mean_ratio_cpu'], index=['SITE_NAME', 'REGION'], aggfunc=np.max), 2)
    df_result = df_pivot.reset_index()
    return df_result

# df = parsing_cpu_load_2g3g()
# df = setDfCpuLoad2g3gUme("UME_SUL")
# print(df)
# df1 = parsing_cpu_load_4g()
# print(df1)
# df =joining_df_4g_ems("EMS5")
# print(df)
# df =joining_df_4g_ems("EMS7")
# print(df)
# df =setDfCpuLoad4gUme("UME_KAL")
# print(df)
# df =setDfCpuLoad4gUme("UME_SUL")
# print(df)
# df = parsing_cpu_load_4g()
# print(df)
# export_to_csv(df,'all_cpuload_4g.csv')
# setDfCpuLoad2g3gUme("UME_KAL")
# print(df)
# export_to_csv(df,'cpu_load_ems5.csv')
