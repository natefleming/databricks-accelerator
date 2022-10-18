from typing import List, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler

from pyspark.sql import functions as F
from pyspark.sql import types as T

from honeycomb._transformations._transformation import _Transformation, _TransformationFactory


class _MinMax(_Transformation):

    def __init__(self,
                 column_name: str,
                 lower_bound: int = 0,
                 upper_bound: int = 1,
                 output_column=None):
        super().__init__(column_name)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.output_column = output_column

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def apply(self, data_frame: DataFrame) -> DataFrame:
        vector_col = _generate_transformation_column_name(
            self.transformation_name(), self.column_name)
        output_column = self.output_column if self.output_column else self.column_name
        assembler = VectorAssembler(inputCols=[self.column_name],
                                    outputCol=vector_col)
        df = assembler.transform(data_frame)
        scaler = MinMaxScaler(min=self.lower_bound,
                              max=self.upper_bound,
                              inputCol=vector_col,
                              outputCol=output_column)
        result_df = scaler.fit(df).transform(df)
        return result_df

    def output_columns(self) -> List[str]:
        return [self.output_column] if self.output_column else []

    def transformation_name(self):
        return 'min_max'


class _MinMaxFactory(_TransformationFactory):

    def __init__(self):
        super().__init__('min_max')

    def from_config(self, spark: SparkSession, column_name: str,
                    config: Dict) -> _Transformation:
        lower_bound = config.get('lower_bound')
        upper_bound = config.get('upper_bound')
        output_column = config.get('output_column')
        return _MinMax(column_name, lower_bound, upper_bound, output_column)
