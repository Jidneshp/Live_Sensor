import sys
from src.logger import logging
from src.exception import CustomException
#from src.utils import upload_data_to_mongodb
from src.pipeline.training_pipeline import TrainPipeline 

if __name__ == "__main__":
    try:
        
        train_pipeline_config = TrainPipeline()
        train_pipeline_config.run_pipeline()
        # file_path="J:\Project\ineuron\Live_sensor\Aps_failure_data.csv"
        # database_name = 'Jidneshp'
        # collection_name= 'APS sensor data'
        
        # upload_data_to_mongodb(file_path, database_name, collection_name)
        
    except Exception as e:
        raise CustomException(e, sys)