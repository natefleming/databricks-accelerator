from typing import List, Any, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory

DEFAULT_NUM_BITS = 256
VALID_NUM_BITS = [224, 256, 384, 512]


class _Encrypt(_Transformation):

    def __init__(self,
                 column_name: str,
                 num_bits: int = DEFAULT_NUM_BITS,
                 output_column: str = None):
        super().__init__(column_name)
        if num_bits not in VALID_NUM_BITS:
            raise ValueError(
                f'Invalid hash size: {num_bits}. Must be one of {VALID_NUM_BITS}'
            )

        self.num_bits = num_bits
        self.output_column = output_column

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        data_frame = data_frame \
            .withColumn(self.transformation_column_name, F.col(self.column_name).cast(T.StringType())) \
            .withColumn(self.transformation_column_name, F.coalesce(self.transformation_column_name, F.lit('null'))) \
            .withColumn(self.transformation_column_name, F.sha2(self.transformation_column_name, self.num_bits))
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        target_column = self.output_column if self.output_column else self.column_name
        result_df = data_frame.drop(self.column_name).withColumnRenamed(
            self.transformation_column_name, target_column)
        return result_df

    def output_columns(self) -> List[str]:
        return [self.output_column] if self.output_column else []

    def validate_self(self, data_frame: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        return self.column_name in df_columns, f"column '{self.column_name}' doest not exist"

    def transformation_name(self):
        return 'encrypt'


class _EncryptFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('encrypt')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        num_bits = int(config.get('num_bits'))
        output_column = config.get('output_column')
        return _Encrypt(column_name, num_bits, output_column)
