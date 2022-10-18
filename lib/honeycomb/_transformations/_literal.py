from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _Literal(_Transformation):

    def __init__(self, column_name: str, value: Any, data_type: str = 'string'):
        super().__init__(column_name)
        self.value = value
        self.data_type = data_type

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        result_df = data_frame.withColumn(
            self.column_name,
            F.lit(self.value).cast(self.data_type))
        return result_df

    def output_columns(self) -> List[str]:
        return [self.column_name] if self.column_name else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column_name not in df_columns, f"column '{self.column_name}' already exists"

    def transformation_name(self):
        return 'literal'


class _LiteralFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('literal')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        value = config.get('value')
        data_type = config.get('data_type')
        return _Literal(column_name, value, data_type)
