import os
import json
import pandas as pd


def export_to_csv(df_res,res_fname):
	curdir = setCurDir()
	df_res.to_csv(curdir+os.sep+'csv_results'+os.sep+res_fname, encoding='utf-8',index=False)
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