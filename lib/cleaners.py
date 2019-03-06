import re
import pandas as pd
import numpy as np

except_space = re.compile(r'[\w ]')

def expand_to_multi_rows(df, col, expanded_col_name):
	"""Expand column with lists to multiple rows
	
	Given a DataFrame, expand column with lists to multiple rows (causes duplication of data)
	
	:param df: A DataFrame
	:type df: pandas.DataFrame
	:param col: column name
	:type col: str
	:param expanded_col_name: name of column for expanded data
	:type expanded_col_name: str
	"""
	expanded = df.apply(lambda x: pd.Series(x[col]), axis=1).stack().reset_index(level=1, drop=True)
	return pd.merge(
		df.copy(deep=True),
		pd.DataFrame(expanded, columns=[expanded_col_name]),
		left_index=True,
		right_index=True,
		how='inner'
	).reset_index(level=0,drop=True)

def create_new_df_dict_col(df, col):
	"""Create a new DataFrame from column with dictionaries
	
	Given a DataFrame that holds dictionary values at col, create a new DataFrame
	with the values of the dictionaries of that column
	
	:param df: A DataFrame
	:type df: pandas.DataFrame
	:param col: Column name
	:type col: str
	"""
	return df[col].apply(pd.Series)

def simplify_education_informationi_(df, start_col, end_col, non_dates=['Present']):
	"""[MODIFIES] Simplifies education from DataFrame to some simple data values
	
	Given DataFrame extracts degree and the duration of the degree (in years)
	Duration of the degree may be NaN in these conditons:
	1. Beginning date is unclear (not a date)
	2. Ending date is unclear (not a date)

	Additionally it will also replace the start_col and end_col values to be
	of type datetime64 or NaT

	See: http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime
	
	:param df: Pandas DataFrame
	:type df: pandas.DataFrame
	:param degree_col: Column where it holds the start of the degree
	 start_col: str
	:param degree_col: Column where it holds the end of the degree
	:type end_col: str
	:param non_dates: List of non date values
	:type non_dates: List
	"""
	start_is_present = df[start_col].isin(non_dates) |\
		df[end_col].isin(non_dates)
	df.loc[start_is_present,(start_col, end_col)] = np.nan

	df.loc[:,start_col] = pd.to_datetime(df[start_col])
	df.loc[:,end_col] = pd.to_datetime(df[end_col])

	delta = df[end_col] - df[start_col]
	df['degree_year_time'] = delta.apply(lambda x: x / np.timedelta64(1, 'Y'))
	return df

def simplify_skills_information_(df, exp_col, extract_pat=r'(\d) year'):
	"""[MODIFIES] Simplifies skill information from DataFrame to simple values 
	
	Given DataFrame with an experience column of type str, exp_col, extract the experience
	(some numerical value) according to a regex extraction pattern. Replaces the experience information
	in exp_col with the numerical value
	
	:param df: Pandas DataFrame
	:type df: pandas.DataFrame
	:param exp_col: Column for the experience
	:type exp_col: str
	:param extract_pat: Regex str, defaults to r'(%d) year'
	:param extract_pat: str, optional
	"""
	df[exp_col] = df[exp_col].str.extract(extract_pat)