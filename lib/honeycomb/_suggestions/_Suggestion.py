from typing import List, Tuple
from abc import ABC, abstractmethod
import random
import string

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


def _generate_suggestion_column_name(suggestion_type, column_name):
    random_suffix = ''.join(
        random.choice(string.ascii_lowercase) for i in range(12))
    return f"__honeycomb__{column_name}_{suggestion_type}_{random_suffix}"


class _Suggestion(ABC):

    def __init__(self, spark: SparkSession, column: T.StructField):
        self.spark = spark
        self.column = column
        self.suggestion_column_name = _generate_suggestion_column_name(
            self.suggestion_name(), column.name)

    @abstractmethod
    def suggestion_name(self):
        pass

    @abstractmethod
    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    @abstractmethod
    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        return False, data_frame

    def can_apply(self) -> bool:
        return True

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column.name in df_columns, f"There is no '{self.column.name}' column"
