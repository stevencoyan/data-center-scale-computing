import pandas as pd
import numpy as np
import argparse

# function to read csv
def read_csv(source):
    data = pd.read_csv(source)
    return data

# function to clean data
def clean_data(data):
    data[['Month', 'Year']] = data['MonthYear'].str.split(' ', expand = True)
    data['Name'] = data['Name'].replace(np.nan, 'Steven')
    data.drop(columns = ['Month', 'Year'], inplace = True)
    return data

# fucntion to load data to csv
def write_csv(data, target):
    data.to_csv(target, index = False)
    return data

# main pipeline if run as script
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # add source and target csv
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print('Starting')

    # read csv
    data = read_csv(args.source)
    new_data = clean_data(data)

    # write csv
    write_csv(new_data, args.target)

    print('Done: Good Job Steven!')


