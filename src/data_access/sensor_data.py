import os
import sys
import json
import numpy as np
import pandas as pd

from typing import Optional

from pymongo.synchronous import collection, database
from src.exception import CustomException
from src.constant.data_base import DATABASE_NAME
from src.configuration.mongodb_connection import MongoDBClient

class SensorData:
    '''
    This class helps export the entire Mongo DB record as pandas DataFrame
    '''

    def __init__(self):
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
            
        except Exception as e:
            raise CustomException(e,sys)
    
    def save_csv_file(self,file_path, collection_name:str, database_name:Optional [str] = None):
        try:
            data_frame = pd.read_csv(file_path)
            data_frame.reset_index(drop=True, inplace=True)
            records =list(json.loads(data_frame.T.to_json()).values)
            
            if database is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            collection.insert_many(records)
            
        except Exception as e:
            raise CustomException(e, sys)
        
    def export_collection_as_dataframe(self, collection_name:str, database_name:Optional[str]=None)->pd.DataFrame:
        '''
        Exports the collection from MongoDB as a data Frame
        '''
        try:
            if database is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            df = pd.DataFrame(list(collection.find()))
            
            if '_id' in df.columns.to_list:
                df = df.drop(columns=['_id'], axis=1)
                
            df.replace({'na':np.nan}, inplace=True)
            
            return df
            
        except Exception as e:
            raise CustomException(e,sys)
    