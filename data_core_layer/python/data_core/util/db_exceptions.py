from enum import Enum
from typing import Optional


class DbErrorCode(Enum):
    """Possible Data Base Error Codes"""
    INTERNAL_DATA_EXCEPTION = 'INTERNAL_DATA_EXCEPTION'
    DATA_INTEGRITY_EXCEPTION = 'DATA_INTEGRITY_EXCEPTION'
    NATURAL_KEY_VIOLATION = 'NATURAL_KEY_VIOLATION'
    FOREIGN_KEY_VIOLATION = 'FOREIGN_KEY_VIOLATION'
    UNHANDLED_ERROR = 'UNHANDLED_ERROR'


class DataCoreErrorCode(Enum):
    """Possible Data Core Error Codes"""
    NO_RECORDS_AVAILABLE = 'NO_RECORDS_AVAILABLE'


class DbException(Exception):
    """
    Base Exception for the Data Core Library

    :var error_code: The exceptions error code to be used to determine what the exception was caused by
    :var error_message: Details on what caused the exception
    """
    error_code: str
    error_message: str

    def __init__(self, error_code: DbErrorCode, error_message: str):
        self.error_code = error_code.value
        self.error_message = error_message
        super().__init__(self.error_message)
        
class RecordNotFoundDbException(DbException):
    """Error Message for when the single read returns zero records"""
  
 
    def __init__(self, *, function_name: Optional[str] = None, table_name: Optional[str] = None, identifier_name: str):
        if function_name:
            error_message = f'No Records found with the corresponding {identifier_name} for the function {function_name}'
        elif table_name:
            error_message = f'No Records found with the corresponding {identifier_name} for the table {table_name}'
        else:
            raise ValueError("RecordNotFoundDbException must be created with either 'function_name' or 'table_name' arguments")
        
        error_code = DataCoreErrorCode.NO_RECORDS_AVAILABLE
        super().__init__(error_code=error_code, error_message=error_message)


 

