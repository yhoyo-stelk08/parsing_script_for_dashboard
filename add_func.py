import os
import json
import pandas as pd

# Define a function to convert SITEID and CELLID to int or float for Linux
def convert_site_cell(column_value, platform):
    if platform == 'Linux':
        return int(float(column_value))
    else:
        return int(column_value)


def export_to_csv(df_res, res_fname):
    curdir = setCurDir()
    df_res.to_csv(curdir+os.sep+'csv_results'+os.sep +
                  res_fname, encoding='utf-8', index=False)
    print('Already export to csv')


def setCurDir():
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    return os.getcwd()


def readConfigFile():
    curdir = setCurDir()
    with open(curdir+os.sep+'conf.json') as f:
        data = json.load(f)
    return data
