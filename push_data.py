import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL=os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi
## trusted certificates ka address deta hai jisse secure connection possible hota hai
ca=certifi.where()

import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def csv_to_json_convertor(self,file_path):
        try:
            ## Read the dataset from source file
            data=pd.read_csv(file_path)
            ## drop their index column from dataset
            data.reset_index(drop=True,inplace=True)
            ## return krta hai list of json
            records=list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def insert_data_mongodb(self,records,database,collection):
        try:
            self.database=database
            self.collection=collection
            self.records=records
            ## iss address wale mongodb server se connect ho jao
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]
            
            self.collection=self.database[self.collection]
            ## yaha pe data json format ko insert kar diya table me approx 11k rows and 31 columns
            self.collection.insert_many(self.records)
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e,sys)
  ## start execution of etl pipeline      
if __name__=='__main__':
    FILE_PATH="Network_Data\phisingData.csv"
    DATABASE="manjesh38"
    Collection="NetworkData"
    networkobj=NetworkDataExtract()
    ## convert data into json format
    records=networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    print(records)
    ## After converting data into json format insert data into mongodb
    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(no_of_records)
        


