import pandas as pd
import os
import glob
from datetime import datetime

def extract_reporate():
	"""
	Extract repo rate data from the CSV file
	"""
	try:
		df = pd.read_csv('HistoricalRateDetail.csv', encoding='utf-8')
		df['date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
		#df.set_index('date', inplace=True)
		df['repo_rate'] = df['Value'].astype(float)
		del df['Date']
		del df['Value']
		return df
	except Exception as e:
		print(f"Error extracting repo rate data: {e}")
		return pd.DataFrame()

def extract_cpi():
	"""
	Extract CPI data from the CSV file
	"""
	try:
		df = pd.read_csv('cpi_history.csv', encoding='utf-8')
		df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
		#df.set_index('date', inplace=True)
		df['inflation_rate'] = df['inflation_rate'].astype(float)
		return df
	except Exception as e:
		print(f"Error extracting repo rate data: {e}")
		return pd.DataFrame()
	
def extract_unemployment():
	"""
	Extract Unemployment Rate data from the CSV file
	"""
	try:
		df = pd.read_csv('LRUN64TTZAQ156S.csv', encoding='utf-8')
		df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
		#df.set_index('date', inplace=True)
		df['unemployment_rate'] = df['unemployment_rate'].astype(float)
		return df
	except Exception as e:
		print(f"Error extracting repo rate data: {e}")
		return pd.DataFrame()
	
def extract_gdp():
	"""
	Extract CPI data from the CSV file
	"""
	try:
		df = pd.read_csv('NGDPRSAXDCZAQ.csv', encoding='utf-8')
		df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
		#df.set_index('date', inplace=True)
		df['gdp'] = df['gdp'].astype(float)
		return df
	except Exception as e:
		print(f"Error extracting repo rate data: {e}")
		return pd.DataFrame()
