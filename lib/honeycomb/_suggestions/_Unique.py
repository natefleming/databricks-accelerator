from typing import Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from honeycomb._suggestions._Suggestion import _Suggestion

LOGGER = logging.getLogger(__name__)


class _Unique(_Suggestion):

    def __init__(self, spark: SparkSession, column: T.StructField):
        super().__init__(spark, column)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        LOGGER.info('_NotNull.prepare_df_for_check')
        return super().prepare_df_for_check(df)

    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        LOGGER.info(f'_Unique.apply: column_name={self.column.name}')
        row_count = data_frame.filter(F.col(
            self.column.name).isNotNull()).count()
        distinct_count = data_frame.select(
            F.countDistinct(
                self.column.name).alias('distinct')).collect()[0].distinct
        result_df = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), T.StructType())
        result: Tuple[bool,
                      DataFrame] = (row_count > 0 and
                                    row_count == distinct_count, result_df)
        LOGGER.info(f'_Unique.apply - returning: {result}')
        return result

    def suggestion_name(self):
        return "unique"
