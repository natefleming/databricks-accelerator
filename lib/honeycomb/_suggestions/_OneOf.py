from typing import Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from honeycomb._suggestions._Suggestion import _Suggestion

LOGGER = logging.getLogger(__name__)

DEFAULT_DISTINCT_COUNT = 5


class _OneOf(_Suggestion):

    def __init__(self,
                 spark: SparkSession,
                 column: T.StructField,
                 distinct_count: int = DEFAULT_DISTINCT_COUNT):
        super().__init__(spark, column)
        self._distinct_count = distinct_count

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        LOGGER.info('_OneOf.prepare_df_for_check')
        return super().prepare_df_for_check(df)

    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        LOGGER.info(f'_OneOf.apply: column_name={self.column.name}')
        result: Tuple[bool, DataFrame] = None
        distinct_count = data_frame.select(
            F.countDistinct(
                self.column.name).alias('distinct')).collect()[0].distinct
        if distinct_count > 0 and distinct_count <= self._distinct_count:
            result = (True,
                      data_frame.agg(
                          F.collect_set(self.column.name).alias('values')))
        else:
            result = (False,
                      self.spark.createDataFrame(
                          self.spark.sparkContext.emptyRDD(), T.StructType()))
        LOGGER.info(f'_OneOf.apply - returning: {result}')
        return result

    def suggestion_name(self):
        return "one_of"
