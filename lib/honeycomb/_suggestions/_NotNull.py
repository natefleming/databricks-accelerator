from typing import Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from honeycomb._suggestions._Suggestion import _Suggestion

LOGGER = logging.getLogger(__name__)


class _NotNull(_Suggestion):

    def __init__(self, spark: SparkSession, column: T.StructField):
        super().__init__(spark, column)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        LOGGER.info('_NotNull.prepare_df_for_check')
        return super().prepare_df_for_check(df)

    def apply(self, data_frame: DataFrame) -> Tuple[bool, DataFrame]:
        LOGGER.info(f'_NotNull.apply: column_name={self.column.name}')
        count = data_frame.filter(F.col(self.column.name).isNull()).count()
        result_df = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), T.StructType())
        result: Tuple[bool, DataFrame] = (count <= 0, result_df)
        LOGGER.info(f'_NotNull.apply - returning: {result}')
        return result

    def suggestion_name(self):
        return "not_null"
