import sys
from src.logger import logging
from src.exception import CustomException
from src.utils import upload_data_to_mongodb

if __name__ == "__main__":
    try:
        file_path="J:\Project\ineuron\Live_sensor\Aps_failure_data.csv"
        database_name = 'Jidneshp'
        collection_name= 'APS sensor data'
        
        upload_data_to_mongodb(file_path, database_name, collection_name)
        
    except Exception as e:
        raise CustomException(e, sys)