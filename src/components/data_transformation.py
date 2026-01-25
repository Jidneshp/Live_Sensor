import sys

import numpy as np
import pandas as pd
from imblearn.combine import SMOTETomek
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, TargetEncoder

from src.logger import logging
from src.exception import CustomException
from src.constant.training_pipeline import TARGET_COLUMN
from src.ml.model.estimator import TargetValueMapping
from src.entity.config_entity import DataTransformationConfig
from src.utils.main_utils import save_numpy_array_data, save_obj
from src.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact


class DataTransformation:
    
    def __init__(self, data_validation_artifact: DataValidationArtifact,
                 data_transformation_config: DataTransformationConfig):
        '''
        data_validation_artifact: Output referrence of DataValidation Artifact
        data_transformation_config:Configuration for data Transformation
        '''
        
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
            
        except Exception as e:
            raise CustomException(e,sys)
        
        
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e,sys)
    
    
    @classmethod
    def get_data_transformer_obj(cls)->Pipeline:
        try:
            robust_sc = RobustScaler()
            simple_imputer = SimpleImputer(strategy='constant', fill_value=0)
            preprocessor = Pipeline(steps=[
                ('imputer', simple_imputer),
                ('scaler', robust_sc)
            ])
            return preprocessor
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def initiate_data_transformation(self,)->DataTransformationArtifact:
        
        try:
            logging.info(f'Starting the Data Transformation Process')
            
            train_df = DataTransformation.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df = DataTransformation.read_data(self.data_validation_artifact.valid_test_file_path)
            
            preprocessor = self.get_data_transformer_obj()
            
            #training DataFrame
            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
            
            target_feature_train_df = train_df[TARGET_COLUMN]
            
            target_feature_train_df = (
                target_feature_train_df.replace(TargetValueMapping().to_dict()).astype(np.int64)
            )
            
            #Testing DataFrame
            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            
            target_feature_test_df = test_df[TARGET_COLUMN]
            
            target_feature_test_df = (
                target_feature_test_df.replace(TargetValueMapping().to_dict()).astype(np.int64)
            )
            
            #Preprocessing of the DATA
            preprocessing_obj = preprocessor.fit(input_feature_train_df)
            
            transformed_input_feature_train_df = preprocessing_obj.transform(input_feature_train_df)
            transformed_input_feature_test_df = preprocessing_obj.transform(input_feature_test_df)
            
            smt = SMOTETomek(sampling_strategy='minority')  # Sampling Method
            
            final_train_input_feature, final_train_target_feature = smt.fit_resample(
                transformed_input_feature_train_df, target_feature_train_df
            )
            
            final_test_input_feature, final_test_target_feature = smt.fit_resample(
                transformed_input_feature_test_df, target_feature_test_df
            )
            
            train_arr = np.c_[final_train_input_feature, np.array(final_train_target_feature)]
            test_arr = np.c_[final_test_input_feature, np.array(final_test_target_feature)]
            
            #Saving numpy array data
            save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path, array=test_arr)
            
            save_obj(self.data_transformation_config.transformed_obj_file_path, preprocessing_obj)
            
            
            #Preparing Artifact
            data_transformation_artifact = DataTransformationArtifact(
            transformed_obj_file_path=self.data_transformation_config.transformed_obj_file_path ,
            transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
            transformed_test_file_path=self.data_transformation_config.transformed_test_file_path  
            )
            
            logging.info(f'Data Transformation Artifact: {data_transformation_artifact}')
            
            return data_transformation_artifact
        
        except Exception as e:
            raise CustomException(e,sys)
