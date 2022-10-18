from typing import List, Tuple, Dict
from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _ConstraintFactory, _Constraint


class _OneOf(_UnaryConstraint):

    def __init__(self, column_name: str, allowed_values: List[str]):
        super().__init__('one_of', column_name)
        self.allowed_values = allowed_values

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(df[self.column_name].isin(*self.allowed_values))

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(~df[self.column_name].isin(*self.allowed_values))


class _OneOfFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('one_of')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        values = list(config['values'])
        return _OneOf(column_name, values)
