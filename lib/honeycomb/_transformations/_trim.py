from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _Trim(_Transformation):

    def __init__(self, column_name: str, output_column: str = None):
        super().__init__(column_name)
        self.output_column = output_column

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        target_column = self.output_column if self.output_column else self.column_name
        result_df = data_frame.withColumn(target_column,
                                          F.trim(F.col(self.column_name)))
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
        return 'trim'


class _TrimFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('trim')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        output_column = config.get('output_column')
        return _Trim(column_name, output_column)
