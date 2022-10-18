from typing import List, Dict

from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _CompoundConstraint, _ConstraintFactory, _Constraint


class _UniqueSet(_CompoundConstraint):

    def __init__(self, column_names: List[str]):
        super().__init__('unique_set', column_names)

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        count_repetitions: DataFrame = df \
            .groupby(*self.column_names) \
            .count() \
            .withColumnRenamed('count', self.constraint_column_name)
        return df.join(count_repetitions, self.column_names, 'left')

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.constraint_column_name} == 1')

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.constraint_column_name} > 1')


class _UniqueSetFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('are_unique')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        column_names = list(config['values'])
        return _UniqueSet(column_names)
