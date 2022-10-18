from typing import Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from honeycomb._suggestions._Suggestion import _Suggestion

LOGGER = logging.getLogger(__name__)


class _Min(_Suggestion):

    def __init__(self, spark: SparkSession, column: T.StructField):
        super().__init__(spark, column)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        LOGGER.info('_Min.prepare_df_for_check')
        return super().prepare_df_for_check(df)

    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        LOGGER.info(f'_Min.apply: column_name={self.column.name}')
        result_df = data_frame.select(
            F.min(F.col(self.column.name)).alias('value')).filter(
                F.col('value').isNotNull())
        result: Tuple[bool, DataFrame] = (result_df.count() > 0, result_df)
        LOGGER.info(f'_Min.apply - returning: {result}')
        return result

    def can_apply(self) -> bool:
        return isinstance(self.column.dataType, T.NumericType)

    def suggestion_name(self):
        return "min"
