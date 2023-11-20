#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine 
from pathlib import Path

def extract_data(source):
    return pd.read_csv(source)

def transform_data(source_csv, target_dir):
    new_data = pd.read_csv(source_csv)
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    new_data['Animal_Key'] = range(1, len(new_data) + 1)
    new_data['Outcome_ID'] = range(1, len(new_data) + 1)
    new_data['Outcome_Type_ID'] = range(1, len(new_data) + 1)   
    new_data['Breed_ID'] = range(1, len(new_data) + 1)
    new_data['ts'] = pd.to_datetime(new_data.DateTime)
    new_data['Date_ID'] = new_data.ts.dt.strftime('%Y%m%d')
    new_data['time'] = new_data.ts.dt.time

    fact_data_f = fact_table(new_data)
    dim_animals_data_f = dim_animals(new_data)
    dim_outcomes_data_f = dim_outcomes(new_data)
    dim_date_data_f = dim_date(new_data)
    

    # create target directory if it doesn't exist
    Path(target_dir).mkdir(parents=True, exist_ok=True)
  
    # # to parquet
    fact_data_f.to_parquet(target_dir + '/fact_table.parquet')
    dim_animals_data_f.to_parquet(target_dir + '/dim_animals.parquet')
    dim_outcomes_data_f.to_parquet(target_dir + '/dim_outcomes.parquet')
    dim_date_data_f.to_parquet(target_dir + '/dim_date.parquet')

    print(fact_data_f.head()); print(dim_animals_data_f.head()); print(dim_outcomes_data_f.head()); print(dim_date_data_f.head())

def fact_table(new_df):
    fact_data = new_df[["Outcome_ID", "Animal_Key", "Date_ID", "time", "Outcome_Type_ID", "Outcome Subtype"]]
    fact_data = fact_data.drop_duplicates(subset = 'Animal_Key')
    return fact_data

def dim_animals(new_df):
    dim_animals_data = new_df[['Animal_Key', 'Name', 'Date of Birth', 'sex', 'Color', 'Animal Type', 'Breed']]
    dim_animals_data = dim_animals_data.drop_duplicates(subset = 'Animal_Key')
    dim_animals_data.columns = ['Animal_Key', 'Name', 'Date_of_Birth', 'sex', 'Color', 'Animal_Type', 'Breed']

    return dim_animals_data
    

def dim_outcomes(new_df):
    outcome_type = new_df[['Outcome Type']]
    outcome_type = outcome_type.drop_duplicates(subset = 'Outcome Type')
    outcome_type['Outcome_Type_ID'] = outcome_type.index + 1
    outcome_type = outcome_type[['Outcome_Type_ID', 'Outcome Type']]
    return outcome_type

def dim_date(new_df):
    dim_date_data = new_df[['Date_ID', 'ts', 'month', 'year']]
    dim_date_data = dim_date_data.drop_duplicates(subset = 'Date_ID')
    return dim_date_data


