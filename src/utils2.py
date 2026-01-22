import sys
import json
import logging
import pandas as pd
from src.config import mongo_client
from src.exception import CustomException

def upload_data_to_mongodb(file_path:str, database_name:str, collect_name:str)->None:
    try:
        df = pd.read_csv(file_path)
        df.reset_index(drop=True, inplace=True)
        json_records = list(json.loads(df.T.to_json()).values())
        
        mongo_client[database_name][collect_name].insert_many(json_records)
        
    except Exception as e:
        raise CustomException(e,sys)