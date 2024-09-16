import json

from typing import Any, List
from attrs import asdict, astuple, define, NOTHING


@define
class DbModel:
    """
    Base Data Core model class providing utilities to enable integration with the database from python code

    To support partial updates make the type of ignorable fields allow for Literal[NOTHING]


    Example Class Extension:

    class DataCoreExampleModel(DbModel):
        required_field_1: str
        optional_field_1: Union[str, Literal[NOTHING]]
        optional_nullable_field_2: Union[str, None, Literal[NOTHING]]

    If a field is set to NOTHING the field will be ignored in any output methods unless the include_all_fields flag is set to True
        - as_dict(include_all_fields: bool)
        - as_json(include_all_fields: bool)
        - as_tuple(include_all_fields: bool)
    """

    def __init__(self, **kwargs):
        # filter out any attributes passed in that are not listed in the model
        filtered = {
            attribute.name: kwargs[attribute.name] for attribute in self.__attrs_attrs__ if attribute.name in kwargs
        }
        self.__attrs_init__(**filtered)

    def as_dict(self, include_all_fields: bool = False):
        return asdict(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields)

    def as_json(self, include_all_fields: bool = False) -> str:
        # TODO Should we add a datetime decoder here or is converting to a string by default valid
        return json.dumps(asdict(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields), default=str)

    def get_column_names(self, include_all_fields: bool = False) -> List[str]:
        return list(self.as_dict(include_all_fields).keys())

    def get_column_names_as_tuple(self, include_all_fields: bool = False) -> tuple:
        return tuple(self.as_dict(include_all_fields).keys())

    def get_column_values(self, include_all_fields: bool = False) -> List[Any]:
        return list(self.as_dict(include_all_fields).values())

    def get_column_values_as_tuple(self, include_all_fields: bool = False) -> tuple:
        return astuple(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields)
    
    #TODO: add a get attributes type list to enhance update helper - caddies story incoming.

@define(slots=False)
class DbModelDicted:
    """
    (Dicted/Non Slotted verison allows for multiple inheritence) Base Data Core model class providing utilities to enable integration with the database from python code

    To support partial updates make the type of ignorable fields allow for Literal[NOTHING]
    
    Example Class Extension:

    class DataCoreExampleModel(DbModel):
        required_field_1: str
        optional_field_1: Union[str, Literal[NOTHING]]
        optional_nullable_field_2: Union[str, None, Literal[NOTHING]]

    If a field is set to NOTHING the field will be ignored in any output methods
        - as_dict()
        - as_json()
        - as_tuple()
    """

    def __init__(self, **kwargs):
        # filter out any attributes passed in that are not listed in the model
        filtered = {
            attribute.name: kwargs[attribute.name] for attribute in self.__attrs_attrs__ if attribute.name in kwargs
        }
        self.__attrs_init__(**filtered)

    def as_dict(self, include_all_fields: bool = False):
        return asdict(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields)

    def as_json(self, include_all_fields: bool = False) -> str:
        # TODO Should we add a datetime decoder here or is converting to a string by default valid
        return json.dumps(asdict(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields), default=str)

    def get_column_names(self, include_all_fields: bool = False) -> List[str]:
        return list(self.as_dict(include_all_fields).keys())

    def get_column_names_as_tuple(self, include_all_fields: bool = False) -> tuple:
        return tuple(self.as_dict(include_all_fields).keys())

    def get_column_values(self, include_all_fields: bool = False) -> List[Any]:
        return list(self.as_dict(include_all_fields).values())

    def get_column_values_as_tuple(self, include_all_fields: bool = False) -> tuple:
        return astuple(self, filter=lambda attr, value: (value != NOTHING) or include_all_fields)