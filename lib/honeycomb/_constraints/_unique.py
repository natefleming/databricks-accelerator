from typing import List, Tuple, Dict
from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _ConstraintFactory, _Constraint


class _Unique(_UnaryConstraint):

    def __init__(self, column_name: str):
        super().__init__('unique', column_name)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        count_repetitions: DataFrame = df \
            .groupby(self.column_name) \
            .count() \
            .withColumnRenamed("count", self.constraint_column_name)
        return df.join(count_repetitions, self.column_name, "left")

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.constraint_column_name} == 1")

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.constraint_column_name} > 1")


class _UniqueFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('unique')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        return _Unique(column_name)
