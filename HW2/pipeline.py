#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine 

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data = data.copy()
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    # new_data['Animal_Key'] = data['Animal Type'].map({'Dog': 0, 'Cat': 1, 'Bird': 2, 'Other': 3})
    new_data['Animal_Key'] = range(1, len(new_data) + 1)
    new_data['Outcome_ID'] = range(1, len(new_data) + 1)
    new_data['Breed_ID'] = range(1, len(new_data) + 1)
    print(new_data.columns)
    print(new_data['Animal_Key'])
    return new_data

def fact_table(new_df, conn):
    fact_data = new_df[["Animal ID", "DateTime", "Animal_Key", "Outcome_ID", "Breed_ID"]]
    fact_data = fact_data.drop_duplicates(subset = 'Animal ID')
    load_data(fact_data, "fact_table", conn)

def dim_animals(new_df, conn):
    dim_animals_data = new_df[['Animal_Key', 'Name', 'Date of Birth', 'sex', 'Color']]
    dim_animals_data = dim_animals_data.drop_duplicates(subset = 'Animal_Key')
    load_data(dim_animals_data, "dim_animals", conn)
    
def dim_outcomes(new_df, conn):
    dim_outcomes_data = new_df[['Outcome_ID', 'Outcome Type', 'Outcome Subtype', 'Age upon Outcome', 'month', 'year']]
    dim_outcomes_data = dim_outcomes_data.drop_duplicates(subset = 'Outcome_ID')
    load_data(dim_outcomes_data, "dim_outcomes", conn)

def dim_breed(new_df, conn):
    dim_breed_data = new_df[['Breed_ID', 'Animal Type', 'Breed']]
    dim_breed_data = dim_breed_data.drop_duplicates(subset = 'Breed_ID')
    load_data(dim_breed_data, "dim_breed", conn)

def load_data(data, table_name, conn):
    data.to_sql(table_name, conn, if_exists='append', index=False)
    # conn.commit()

if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
#    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    db_url = "postgresql+psycopg2://steven:Steven12@db:5432/shelter"
    conn = create_engine(db_url)

    print("Start")
    df = extract_data(args.source)
    new_df = transform_data(df)
    print(new_df.shape)
    dim_animals(new_df, conn)
    dim_outcomes(new_df, conn)
    dim_breed(new_df, conn)
    fact_table(new_df, conn)
    print("Complete")