from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _ToDate(_Transformation):

    def __init__(self,
                 column_name: str,
                 format: str,
                 output_column: str = None):
        super().__init__(column_name)
        self.format = format
        self.output_column = output_column

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        target_column = self.output_column if self.output_column else self.column_name
        if format:
            result_df = data_frame.withColumn(
                target_column,
                F.to_date(
                    F.col(self.column_name).cast(T.StringType()), self.format))
        else:
            result_df = data_frame.withColumn(
                target_column,
                F.to_date(F.col(self.column_name).cast(T.StringType())))
        return result_df

    def output_columns(self) -> List[str]:
        return [self.output_column] if self.output_column else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        is_correct: Tuple[bool, str] = (True, '')
        if self.column_name in df_columns:
            is_correct = True, f"column '{self.column_name}' does not exist"
        elif self.output_column and self.output_column not in df_columns:
            is_correct = True, f"column '{self.output_column}' already exists"

        return is_correct

    def transformation_name(self):
        return 'to_date'


class _ToDateFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('to_date')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        format = config.get('format')
        output_column = config.get('output_column')
        return _ToDate(column_name, format, output_column)
