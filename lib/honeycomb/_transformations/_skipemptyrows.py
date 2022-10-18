from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _SkipEmptyRows(_Transformation):

    def __init__(self):
        super().__init__(None)

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        result_df = data_frame.na.drop(how='all')
        return result_df

    def transformation_name(self):
        return 'skip_empty_rows'

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return True, None


class _SkipEmptyRowsFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('skip_empty_rows')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        return _SkipEmptyRows()
