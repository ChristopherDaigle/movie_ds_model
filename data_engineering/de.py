import csv
import json
import os
from tqdm import tqdm
import pandas as pd
import numpy as np


def create_jsons():
    """
    Function to translate csvs made by dl_tmdb_data.py into JSON
    Note: This was built with the idea the file sizes would be smaller, it doesnt seem they are
    :return: json versions of any csv-s in the ../data/ directory
    """
    os.chdir("..")

    data_dir = "data/"
    csv_files = []
    files = os.listdir(data_dir)
    for file in files:
        if '.csv' in file:
            csv_files.append(file)
    print(csv_files)

    os.chdir(data_dir)
    for file in tqdm(csv_files, ascii=True, desc="Transforming csv-s to json-s"):
        print(file)
        csv_path = file
        json_name = file.replace(".csv", ".json")
        data = {}

        with open(csv_path) as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for rows in tqdm(csv_reader, ascii=True, desc="Indexing rows"):
                id_s = rows['id']
                data[id_s] = rows

        with open(json_name, 'w') as json_file:
            json_file.write(json.dumps(data, indent=4))

    return


def string_array_clean(df_col, element):
    """
    Function to clean columns of dataframe that hold strings of arrays of dictionaries
    :param df_col: series from pandas dataframe (e.g. df[column])
    :param element: key in dictionary of which a list should be built

    :return pandas series with string turned into list of elements and empty lists as np.nan
    """
    df_col = df_col.apply(lambda x: [i[element] for i in json.loads(x.replace("'", ""))])   # Create list
    df_col = df_col.apply(lambda x: np.nan if len(x) == 0 else x)                           # Replace empty list

    return df_col


def uniques_from_list(df_col):
    """
    Function to create dictionary with number of times an item appears in a column made of lists
    :param df_col: pandas series from dataframe, e.g. df[col]
    :return count_dict: dictionary with distinct elements as keys and number of times they appear as values
    """
    count_dict = {}
    for index in range(df_col.shape[0]):
        obs = df_col[index]
        if obs is not np.nan:
            for item in obs:
                item = str(item)
                if item in count_dict.keys():
                    count_dict[item] +=1
                else:
                    count_dict[item] = 1
    return count_dict
