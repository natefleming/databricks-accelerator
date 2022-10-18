from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _RegexReplace(_Transformation):

    def __init__(self,
                 column_name: str,
                 pattern: str,
                 replacement: str,
                 output_column: str = None):
        super().__init__(column_name)
        self.pattern = pattern
        self.replacement = replacement
        self.output_column = output_column

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        target_column = self.output_column if self.output_column else self.column_name
        result_df = data_frame.withColumn(
            target_column,
            F.regexp_replace(self.column_name, self.pattern, self.replacement))
        return result_df

    def output_columns(self) -> List[str]:
        return [self.output_column] if self.output_column else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column_name in df_columns, f"column '{self.column_name}' doest not exist"

    def transformation_name(self):
        return 'replace'


class _RegexReplaceFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('replace')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        pattern = config.get('pattern')
        replacement = config.get('replacement')
        output_column = config.get('output_column')
        return _RegexReplace(column_name, pattern, replacement, output_column)
