import os
import sys
import dill
import yaml
import numpy as np
import pandas as pd
from src.logger import logging
from src.exception import CustomException


def read_yaml(file_path:str)->dict:
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
        
    except Exception as e:
        raise CustomException(e, sys)
    

def write_yaml(file_path:str, content:object, replace:bool=False)->None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as file:
            yaml.dump(content, file)
    except Exception as e:
        raise CustomException(e,sys)
    

def save_numpy_array_data(file_path:str, array:np.array):
    '''
    Save numpy array data to file 
    '''
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, 'wb') as f:
            np.save(f, array)
    except Exception as e:
        raise CustomException(e,sys)
    

def load_numpy_array_data(file_path:str)->np.array:
    '''
    Load Numpy array data from file
    '''
    try:
        with open(file_path, 'rb') as f:
            return np.load(f)
    except Exception as e:
        raise CustomException(e,sys)
    
    
def save_obj(file_path:str, obj:object)-> None:
    try:
        logging.info("Entered the save_obj method in utils")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            dill.dump(obj, f)
        logging.info("Exited the save_obj method of Utils")
    except Exception as e:
        raise CustomException(e,sys)
    
    
def load_obj(file_path:str)->object:
    try:
        if not os.path.exists(file_path):
            raise Exception (f'The file: {file_path} does not exist')
        with open (file_path, 'rb') as f:
            return dill.load(f)
        
    except Exception as e:
        raise CustomException(e,sys)        