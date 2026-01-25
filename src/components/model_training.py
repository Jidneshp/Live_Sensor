import os, sys
from src.logger import logging
from src.exception import CustomException
from src.utils.main_utils import load_numpy_array_data

from xgboost import XGBClassifier
from src.entity.config_entity import ModelTrainerConfig
from src.utils.main_utils import save_obj, load_obj
from src.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact

from src.ml.model.estimator import SensorModel
from src.ml.metric.classification_report import get_classification_score


class ModelTrainer:
    def __init__(self, model_trainer_config:ModelTrainerConfig, 
                 data_transformation_artifact:DataTransformationArtifact):
        
        try:
            
            self.model_trainer_config=model_trainer_config
            self.data_transformation_artifact=data_transformation_artifact
            
        except Exception as e:
            raise CustomException(e,sys)
        
    def perform_hyper_parameter_tuning(self):... #pass
        
    def train_model(self, x_train, y_train):
        try:
            xgb = XGBClassifier()
            xgb.fit(x_train, y_train)
            
            return xgb
        except Exception as e:
            raise CustomException(e,sys)
        
    def initiate_model_trainer(self)-> ModelTrainerArtifact:
        try:
            train_file_path=self.data_transformation_artifact.transformed_train_file_path
            test_file_path=self.data_transformation_artifact.transformed_test_file_path
            
            #Loading train and test array
            train_arr = load_numpy_array_data(train_file_path)
            test_arr = load_numpy_array_data(test_file_path)
            
            #Spliting train and test DATA
            
            X_train, y_train, X_test, y_test = (
                train_arr[:, :-1],
                train_arr[:, -1],
                test_arr[:, :-1],
                test_arr[:, -1],
            )
            
            logging.info(f'Train Test split done')
            
            model = self.train_model(X_train, y_train)
            
            logging.info(f'Model Training Completed')
            
            y_train_pred = model.predict(X_train)
            
            y_pred = model.predict(X_test)
            
            classification_test_metric = get_classification_score(y_test, y_pred)
            
            if classification_test_metric.f1_score <= self.model_trainer_config.expected_accuracy:
                raise Exception('Trained model does not provide expected accuracy')
            
            classification_train_metric = get_classification_score(y_train, y_train_pred)
            
            diff = abs(classification_train_metric.f1_score - classification_test_metric.f1_score)
            
            if diff > self.model_trainer_config.overfitting_underfitting_threshold:
                raise Exception (f'Modelis not good try experimenting')
            
            preprocessor = load_obj(file_path=self.data_transformation_artifact.transformed_obj_file_path)
            
            model_dir_path = os.path.dirname(self.model_trainer_config.trained_model_file_path)
            os.makedirs(model_dir_path, exist_ok=True)
            
            sensor_model = SensorModel(preprocessor, model)
            save_obj(self.model_trainer_config.trained_model_file_path, obj=sensor_model)
            
            # Model Trainer Artifact
            
            model_trainer_artifact = ModelTrainerArtifact(
                trainer_model_path=self.model_trainer_config.trained_model_file_path,
                train_metric_artifact=classification_train_metric,
                test_metric_artifact=classification_test_metric
            )
            
            logging.info(f'Model Trainer Artifact: {model_trainer_artifact}')
            
            return model_trainer_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
        
            