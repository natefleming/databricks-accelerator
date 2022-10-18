from typing import Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from honeycomb._suggestions._Suggestion import _Suggestion

LOGGER = logging.getLogger(__name__)


class _LengthBetween(_Suggestion):

    def __init__(self, spark: SparkSession, column: T.StructField):
        super().__init__(spark, column)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        LOGGER.info('_LengthBetween.prepare_df_for_check')
        return super().prepare_df_for_check(df)

    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        LOGGER.info(f'_LengthBetween.apply: column_name={self.column.name}')
        result_df = data_frame.agg(
            F.min(F.length(F.col(self.column.name))).alias('lower_bound'),
            F.max(F.length(F.col(self.column.name))).alias(
                'upper_bound')).filter((F.col('lower_bound').isNotNull()) &
                                       (F.col('upper_bound').isNotNull()))
        result: Tuple[bool, DataFrame] = (result_df.count() > 0, result_df)
        LOGGER.info(f'_LengthBetween.apply - returning: {result}')
        return result

    def can_apply(self) -> bool:
        return isinstance(self.column.dataType, T.StringType)

    def suggestion_name(self):
        return "length_between"
