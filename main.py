import sys
from src.exception import CustomException

if __name__ == "__main__":
    try:
        a = 10
        b = 0
        print(a/b)
        
    except Exception as e:
        raise CustomException(e, sys)