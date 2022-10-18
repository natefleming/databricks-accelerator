from typing import List, Tuple, Dict
from abc import ABC, abstractmethod
import random
import string

from pyspark.sql import DataFrame, SparkSession


def _generate_transformation_column_name(transformation_type, column_name):
    random_suffix = ''.join(
        random.choice(string.ascii_lowercase) for i in range(12))
    return f"__honeycomb__{column_name}_{transformation_type}_{random_suffix}"


class _Transformation(ABC):

    def __init__(self, column_name: str):
        self.column_name = column_name
        self.transformation_column_name = _generate_transformation_column_name(
            self.transformation_name(), column_name)

    @abstractmethod
    def transformation_name(self):
        pass

    @abstractmethod
    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    @abstractmethod
    def apply(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def output_columns(self) -> List[str]:
        return []

    def remove_columns(self) -> List[str]:
        return []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column_name in df_columns, f"There is no '{self.column_name}' column"


class _TransformationFactory(ABC):

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    @abstractmethod
    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        pass
