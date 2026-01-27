import os, sys
import pandas as pd
import numpy as np

from src.components import model_training
from src.logger import logging
from src.exception import CustomException

from src.ml.model.estimator import ModelResolver
from src.ml.model.estimator import TargetValueMapping
from src.constant.training_pipeline import TARGET_COLUMN
from src.utils.main_utils import save_obj, load_obj, write_yaml

from src.ml.metric.classification_report import get_classification_score
from src.entity.config_entity import ModelTrainerConfig, ModelEvaluationConfig
from src.entity.artifact_entity  import DataValidationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact


class ModelEvaluation:
    def __init__(self, model_eval_config:ModelEvaluationConfig, 
                 data_validation_artifact:DataValidationArtifact,
                 model_training_artifact:ModelTrainerArtifact):
        
        try:
            self.model_eval_config = model_eval_config
            self.data_validation_artifact = data_validation_artifact
            self.model_training_artifact = model_training_artifact
            
        except Exception as e:
            raise CustomException(e,sys)
        
    def initiate_model_evaluation(self)->ModelEvaluationArtifact:
        try:
            valid_train_file_path = self.data_validation_artifact.valid_train_file_path
            valid_test_file_path = self.data_validation_artifact.valid_test_file_path
            
            #Creating Dataframe of Train and Test file
            train_df = pd.read_csv(valid_train_file_path)
            test_df = pd.read_csv(valid_test_file_path)
            
            df = pd.concat([train_df,  test_df])
            
            y_true = df[TARGET_COLUMN]
            
            y_true = y_true.replace(TargetValueMapping().to_dict()).astype(np.int64)
            
            df.drop(TARGET_COLUMN, axis=1, inplace=True)
            
            train_model_file_path = self.model_training_artifact.trainer_model_path
            model_resolver = ModelResolver()
            
            is_model_accepted=True
            
            if not model_resolver.does_model_exists():
            
                model_eval_artifacrt = ModelEvaluationArtifact(
                    is_model_acceptable=is_model_accepted,
                    improved_accuracy=None,
                    best_model_path=None,
                    trained_model_path=train_model_file_path,
                    train_model_metric_artifact=self.model_training_artifact.test_metric_artifact,
                    best_model_metric_artifact=None
                )
                logging.info(f'Model Evaluation Artifact: {model_eval_artifacrt}')
                return model_eval_artifacrt
            
            latest_model_path = model_resolver.get_best_model_path()
            
            latest_model = load_obj(file_path=latest_model_path)
            train_model = load_obj(file_path=train_model_file_path)
            
            y_train_pred = train_model.predict(df)
            y_latest_pred = latest_model.predict(df)
            
            trained_metric = get_classification_score(y_true, y_train_pred)
            latest_metric=get_classification_score(y_true, y_latest_pred)
            
            improved_accuracy = trained_metric.f1_score - latest_metric.f1_score
            
            if self.model_eval_config.change_threshold < improved_accuracy:
                # 0.02 < 0.03
                is_model_accepted=True
            else:
                is_model_accepted=False
                
            model_evaluation_artifact = ModelEvaluationArtifact(
                    is_model_acceptable=is_model_accepted,
                    improved_accuracy=improved_accuracy,
                    best_model_path=latest_model_path,
                    trained_model_path=train_model_file_path,
                    train_model_metric_artifact=trained_metric,
                    best_model_metric_artifact=latest_metric
            )
            model_eval_report = model_evaluation_artifact.__dict__
            
            # Savinig the report
            write_yaml(self.model_eval_config.report_file_path, model_eval_report)
            logging.info(f'Model Evaluation Artifact: {model_evaluation_artifact}')
            
            return model_evaluation_artifact
        
        except Exception as e:
            raise CustomException(e,sys) 
    