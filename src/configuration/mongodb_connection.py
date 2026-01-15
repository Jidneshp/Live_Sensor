import os
import sys
import logging
import pymongo
import certifi
ca = certifi.where()
from dotenv import load_dotenv
from src.logger import logging
from src.constant.env_variable import MONGODB_URL_KEY
from src.constant.data_base import DATABASE_NAME, COLLECTION_NAME 
load_dotenv()

class MongoDBClient:
    client=None
    
    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                logging.info(f"Retrieved MongoDB URL: {mongo_db_url}")
                
                if 'localhost' in mongo_db_url:
                    MongoDBClient.client = pymongo.MongoClient(mongo_db_url)
                else:
                    MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca) #Tls/SSl
                    
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            
        except Exception as e:
            logging.error(f"Error Initializing MongoDB Client: {e}")
            raise 
                    
                    