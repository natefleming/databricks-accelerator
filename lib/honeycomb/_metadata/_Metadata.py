from typing import List, Tuple, Any
from abc import ABC, abstractmethod
import random
import string

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


class _Metadata(ABC):

    def __init__(self, name: str, value: Any):
        self.name = name
        self.value = value

    def metadata_name(self) -> str:
        return self.name

    def metadata_value(self) -> Any:
        return self.value

    def apply(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return True, None


class _ColumnMetadata(_Metadata):

    def __init__(self, column_name: str, name: str, value: Any):
        super().__init__(name, value)
        self.column_name = column_name

    def apply(self, data_frame: DataFrame) -> DataFrame:
        found = next((c for c in data_frame.schema
                      if c.name.lower() == self.column_name.lower()), None)
        existing = found.metadata
        metadata = {self.name: self.value}
        data_frame = data_frame.withColumn(
            self.column_name,
            F.col(self.column_name).alias('', metadata={
                **existing,
                **metadata
            }))
        return data_frame

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column_name.lower() in [
            c.lower() for c in df_columns
        ], f"There is no '{self.column_name}' column"


class _DatasetMetadata(_Metadata):

    def __init__(self, name: str, value: Any):
        super().__init__(name, value)
