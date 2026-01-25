import os, sys

from src.components import data_validation
from src.components import data_transformation
from src.logger import logging
from src.exception import CustomException
from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation 
from src.components.data_transformation import DataTransformation
from src.entity.artifact_entity import DataIngestionArtifiact
from src.entity.artifact_entity import DataIngestionArtifiact, DataValidationArtifact
from src.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig, DataValidationConfig, DataTransformationConfig

class TrainPipeline:
    
    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()
        
    def start_data_ingestion(self)->DataIngestionArtifiact:
        try:
            self.data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            
            logging.info('Starting Data Ingestion')
            
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            
            logging.info(f'Data Ingestion Completed and artifact: {data_ingestion_artifact}')
            
            return data_ingestion_artifact
            
        except Exception as e:
            raise CustomException(e,sys)


    def start_data_validation(self, data_ingestion_artifact:DataIngestionArtifiact)->DataValidationArtifact:
        try:
            data_validation_config = DataValidationConfig(training_pipeline_config=self.training_pipeline_config)
            
            data_validation = DataValidation(data_ingestion_artifact=data_ingestion_artifact, data_validation_config=data_validation_config)
            
            data_validation_artifact = data_validation.initiate_data_validation()
            
            return data_validation_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
    
    
    def start_data_transformation(self, data_validation_artifact:DataValidationArtifact):
        try:
            data_transformation_config = DataTransformationConfig(train_pipeline_config=self.training_pipeline_config)
            data_transformation = DataTransformation(data_validation_artifact=data_validation_artifact, data_transformation_config=data_transformation_config)
            
            data_transformation_artifact = data_transformation.initiate_data_transformation()
            
            return data_transformation_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
        
        
    def run_pipeline(self):
        try:
            data_ingestion_artifact:DataIngestionArtifiact = self.start_data_ingestion()
            
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            
            data_transformation_artifact = self.start_data_transformation(data_validation_artifact=data_validation_artifact)
            
        except Exception as e:
            raise CustomException(e,sys)