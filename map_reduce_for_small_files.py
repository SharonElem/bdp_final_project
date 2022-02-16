# general
import os

import lithops
import csv
import sqlite3

# ml
import pandas as pd
import numpy as np

import time

def inverted_map(document_name, column_index):
    # set the config file to read from the bucket
    config = {'lithops': {'storage_config': 'ibm_cos', 'log_level': 'DEBUG'},
              'ibm_cos':
                  {'storage_bucket': 'cloud-object-storage-tr-cos-standard-vda',
                   'region': 'eu-de',
                   'access_key': '8c96b0e415534708944b9fafff14a539',
                   'secret_key': '79eed0eddd1c980a30c388f96c69ad6a587d47ad41e36a19'
                   }
              }
    storage = lithops.Storage(config=config)
    # get the csv file from the bucket
    csv_file = storage.get_object('cloud-object-storage-tr-cos-standard-6xd', document_name)
    content_to_list = csv_file.decode("utf-8").split('\r\n')
    # create header
    key_csv_map = []
    # for each row in the file append the roe to key_csv_map
    for row in content_to_list:
        row = row.split(',')
        key_csv_map.append((f"{row[column_index - 1]}", document_name))
    return key_csv_map


def inverted_map_for_small_files(documents):
    # set the config file to read from the bucket
    config = {'lithops': {'storage_config': 'ibm_cos', 'log_level': 'DEBUG'},
              'ibm_cos':
                  {'storage_bucket': 'cloud-object-storage-tr-cos-standard-vda',
                   'region': 'eu-de',
                   'access_key': '8c96b0e415534708944b9fafff14a539',
                   'secret_key': '79eed0eddd1c980a30c388f96c69ad6a587d47ad41e36a19'
                   }
              }
    storage = lithops.Storage(config=config)
    # create header
    key_csv_map = []
    for document in documents:
        document_name, column_index = document[0], document[1]
        # get the csv file from the bucket
        csv_file = storage.get_object('cloud-object-storage-tr-cos-standard-6xd', document_name)
        content_to_list = csv_file.decode("utf-8").split('\r\n')
        # for each row in the file append the roe to key_csv_map
        for row in content_to_list:
            row = row.split(',')
            key_csv_map.append((f"{row[column_index - 1]}", document_name))
    return key_csv_map


def inverted_index(value, documents):
    # convert the documents to array
    documents_arr = documents.split(',')
    # remove duplicates
    documents_unique = np.unique(documents_arr)
    # insert the value to the documents unique array in index 0
    result = np.insert(documents_unique, 0, value)
    return result


class MapReduceServerlessEngine():
    def __init__(self):
        # Create empty variables for the map function and reduce functions.
        # Those vars will be set in the execute functions
        self.map_function, self.reduce_function = None, None
        # Vars for the db connection
        self.db_connection = sqlite3.connect('map_reduce_db.db')
        self.cursor = self.db_connection.cursor()
        self.fexec = lithops.FunctionExecutor(log_level='DEBUG')
        self.parts_number = os.environ.get('CHUNKS_NUMBER', 20)

    def execute(self, input_data, map_function, reduce_function):
        # set the map and reduce vars
        self.map_function, self.reduce_function = map_function, reduce_function
        # call the map of the function executer to run the map function
        self.fexec.map(map_function, input_data)
        results = self.fexec.get_result()
        # save the results to the db
        self.save_serverless_functions_results_to_db(results)
        # get all the data sorted
        sorted_data = self.get_sorted_data()
        # here we are using again the map function and not the reduce because we are sorting the data by ourselves
        results = self.fexec.get_result(self.fexec.map(reduce_function, sorted_data))
        self.db_connection.close()
        return results

    def execute_for_small_files(self, input_data, map_function, reduce_function):
        # set the map and reduce vars
        self.map_function, self.reduce_function = map_function, reduce_function
        # divide the input data into parts based on the chunks number set by the user
        new_input_data = self.divide_small_files_into_parts(input_data)
        # call the map of the function execute to run the map function
        self.fexec.map(map_function, new_input_data)
        results = self.fexec.get_result()
        # save the results to the db
        self.save_serverless_functions_results_to_db(results)
        # get all the data sorted
        sorted_data = self.get_sorted_data()
        # here we are using again the map function and not the reduce because we are sorting the data by ourselves
        results = self.fexec.get_result(self.fexec.map(reduce_function, sorted_data))
        self.db_connection.close()
        return results

    def save_serverless_functions_results_to_db(self, results):
        # for each result the map returen we will save it to the temp_results table
        for result in results:
            result_pd = pd.DataFrame(result, columns=['key', 'value'])
            result_pd.to_sql('temp_results', self.db_connection, if_exists='append', index=False)

    def get_sorted_data(self):
        self.cursor.execute("""
                            SELECT key,GROUP_CONCAT(value)
                            FROM temp_results
                            GROUP BY key
                            ORDER BY key;
                            """)
        rows = self.cursor.fetchall()
        return rows

    def divide_small_files_into_parts(self, input_data):
        # get the wanted number of parts in the new input list
        input_data_partition_len = self.calculate_input_data_partition(input_data)
        # return new list of contacted parts
        return [input_data[i:i + input_data_partition_len] for i in range(0, len(input_data), input_data_partition_len)]

    def calculate_input_data_partition(self, input_data):
        # calculate based on the number of wanted parts the new input
        partition_len = int(len(input_data) / self.parts_number)
        # if the input doesn't divide in the number of chunks we will add another chunks that will have less files
        if len(input_data)%self.parts_number >= 1:
            partition_len += 1
        return partition_len


input_data = [(f"myCSV{i}.csv", 1) for i in range(100)]
mapreduce = MapReduceServerlessEngine()
# print(results)
start_time = time.time()
results_old =  mapreduce.execute(input_data, inverted_map, inverted_index)
print("in the old way it took --- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
results = mapreduce.execute_for_small_files(input_data, inverted_map_for_small_files, inverted_index)
print("in the new way it took --- %s seconds ---" % (time.time() - start_time))
