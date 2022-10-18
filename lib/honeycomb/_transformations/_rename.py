from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory
import honeycomb.schema


class _Rename(_Transformation):

    def __init__(self, spark: SparkSession, column_name: str,
                 new_column_name: str):
        super().__init__(column_name)
        self.spark = spark
        self.new_column_name = new_column_name

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        result_df = honeycomb.schema.rename_nested_column(
            self.spark, data_frame, self.column_name, self.new_column_name)
        return result_df

    def output_columns(self) -> List[str]:
        return [self.new_column_name] if '.' not in self.column_name else []

    def remove_columns(self) -> List[str]:
        return [self.column_name] if '.' not in self.column_name else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.new_column_name not in df_columns, f"column '{self.new_column_name}' already exists"

    def transformation_name(self):
        return 'rename'


class _RenameFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('rename')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        new_column_name = config.get('to')
        return _Rename(spark, column_name, new_column_name)
