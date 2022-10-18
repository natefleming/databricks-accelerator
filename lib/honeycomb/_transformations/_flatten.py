from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _Flatten(_Transformation):

    def __init__(self, column_name: str, prefix: str = None):
        super().__init__(column_name)
        self.new_cols = []
        self.prefix = prefix if prefix else column_name.replace('.', '_')

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        target_column = self.column_name.lower()
        flatten_col = F.col(f"{target_column}.*")
        original_cols = data_frame.columns
        select_cols = [F.col(c) for c in original_cols] + [flatten_col]
        result_df: DataFrame = data_frame.select(select_cols)
        result_df = result_df.select([
            F.col(c)
            if c in original_cols else F.col(c).alias(f"{self.prefix}_{c}")
            for c in result_df.columns
        ])
        self.new_cols = [c for c in result_df.columns if c not in original_cols]
        return result_df

    def output_columns(self) -> List[str]:
        return self.new_cols if self.new_cols else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        column_name = self.column_name.split(".", 1)[0]
        return column_name in df_columns, f"column '{column_name}' doest not exist"

    def transformation_name(self):
        return 'flatten'


class _FlattenFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('flatten')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        prefix = config.get('prefix')
        return _Flatten(column_name, prefix)
