from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _Drop(_Transformation):

    def __init__(self, column_name: str):
        super().__init__(column_name)

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        result_df = data_frame.drop(F.col(self.column_name))
        return result_df

    def output_columns(self) -> List[str]:
        return []

    def remove_columns(self) -> List[str]:
        return [self.column_name]

    def transformation_name(self):
        return 'drop'


class _DropFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('drop')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        return _Drop(column_name)
