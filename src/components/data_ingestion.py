import os
import sys
from pandas import DataFrame

from src.data_access import sensor_data
from src.logger import logging
from src.exception import CustomException
from src.data_access.sensor_data import SensorData
from sklearn.model_selection import train_test_split
from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifiact 

from src.utils.main_utils import read_yaml
from src.constant.training_pipeline import SCHEMA_FILE_PATH

class DataIngestion:
    def __init__(self, data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
            self._schema_config = read_yaml(SCHEMA_FILE_PATH)
            
        except Exception as e:
            raise CustomException(e,sys)
    
    def export_data_in_feature_store(self) -> DataFrame:
        '''
        export MongoDB Collection record as Data frame into feature
    
        '''
        
        try:
            logging.info('Exporting data from MongoDB to Feature Store')
            
            sensor_data = SensorData()
            
            dataframe = sensor_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            
            if dataframe.empty:
                raise Exception(f"No data found in MongoDB collection: {self.data_ingestion_config.collection_name}")
            
            logging.info(f"Exported dataframe shape: {dataframe.shape}")
            
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            
            # Creating Folder
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe
        
        except Exception as e:
            raise CustomException(e,sys)
        
    def split_data_into_train_test(self, dataframe:DataFrame)-> None:
        try:
            if dataframe.empty:
                raise Exception("Cannot split empty dataframe. Please ensure data is available in MongoDB collection.")
            
            logging.info(f"Performing Train and Test split on the Data Set. Dataframe shape: {dataframe.shape}")
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            
            logging.info("Train Test split done")
            
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            logging.info(f'Exporting train and Test file path')
            
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )
            
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )
            
            logging.info(f"Exported Train test file path")
            
        except Exception as e:
            raise CustomException(e,sys)
        
    def initiate_data_ingestion(self)-> DataIngestionArtifiact:
        try:
            dataframe = self.export_data_in_feature_store()
            
            # Drop columns if specified in schema
            if 'drop_columns' in self._schema_config and self._schema_config['drop_columns']:
                dataframe = dataframe.drop(self._schema_config['drop_columns'], axis=1)
                logging.info(f"Dataframe shape after dropping columns: {dataframe.shape}")
            
            self.split_data_into_train_test(dataframe=dataframe)
            
            data_ingestion_artifact = DataIngestionArtifiact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            
            return data_ingestion_artifact
        
        except Exception as e:
            raise CustomException(e,sys)