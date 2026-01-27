import sys
from src.components.model_pusher import ModelPusher
from src.logger import logging
from src.exception import CustomException
from src.constant.training_pipeline import SAVED_MODEL_DIR

from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation 
from src.components.data_transformation import DataTransformation
from src.components.model_training import ModelTrainer
from src.components.model_evaluation import ModelEvaluation


from src.entity.artifact_entity import (DataIngestionArtifiact, DataValidationArtifact, 
                                        DataTransformationArtifact, ModelTrainerArtifact,
                                        ModelEvaluationArtifact)
from src.entity.config_entity import (ModelEvaluationConfig, ModelPusherConfig, TrainingPipelineConfig, 
                                     DataIngestionConfig, DataValidationConfig, 
                                     DataTransformationConfig, ModelTrainerConfig)

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
        
    def start_model_training(self, data_transformation_artifact: DataTransformationArtifact):
        try:
            model_training_config = ModelTrainerConfig(training_pipeline_config=self.training_pipeline_config)
            
            model_training = ModelTrainer(model_trainer_config=model_training_config, data_transformation_artifact=data_transformation_artifact)
            
            model_training_artifact = model_training.initiate_model_trainer()
            
            return model_training_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
        
    def start_model_evaluation(self, data_validation_artifact:DataValidationArtifact,
                               model_training_artifact:ModelTrainerArtifact):
        try:
            model_eval_config = ModelEvaluationConfig(training_pipeline_config=self.training_pipeline_config)
            
            model_evaluation = ModelEvaluation(model_eval_config, data_validation_artifact, model_training_artifact)
            
            model_evaluation_artifact = model_evaluation.initiate_model_evaluation()
            
            return model_evaluation_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
        
    def start_model_pusher(self, model_eval_artifact:ModelEvaluationArtifact):
        try:
            model_pusher_config = ModelPusherConfig(self.training_pipeline_config)
            model_pusher = ModelPusher(model_pusher_config, model_eval_artifact)
            
            model_pusher_artifact = model_pusher.initiate_model_pusher()
            
            return model_pusher_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
        
        
    def run_pipeline(self):
        try:
            data_ingestion_artifact:DataIngestionArtifiact = self.start_data_ingestion()
            
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            
            data_transformation_artifact = self.start_data_transformation(data_validation_artifact=data_validation_artifact)
            
            model_training_artifact = self.start_model_training(data_transformation_artifact=data_transformation_artifact)
            
            model_eval_artifact = self.start_model_evaluation(data_validation_artifact, model_training_artifact)
            
            if not model_eval_artifact.is_model_acceptable:
                raise Exception('Trained Model is not better than the best model')
            
            model_eval_artifact = self.start_model_pusher(model_eval_artifact)
            
        except Exception as e:
            raise CustomException(e,sys)