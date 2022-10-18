from typing import List, Tuple, Dict

from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _ConstraintFactory, _Constraint


class _TextLength(_UnaryConstraint):

    def __init__(self, column_name: str, lower_bound: int, upper_bound: int):
        super().__init__('text_length', column_name)
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(
            f'LENGTH({self.column_name}) >= {self._lower_bound} AND LENGTH({self.column_name}) <= {self._upper_bound}'
        )

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(
            f'LENGTH({self.column_name}) < {self._lower_bound} OR LENGTH({self.column_name}) > {self._upper_bound}'
        )

    def validate_self(self, df: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        parent_validation_result = super().validate_self(df, df_columns)
        if not parent_validation_result[0]:
            return parent_validation_result

        column_type = [
            dtype for name, dtype in df.dtypes if name == self.column_name
        ][0]
        if column_type != 'string':
            return False, f'Column {self.column_name} is not a string.'
        else:
            return self._lower_bound <= self._upper_bound, f'Upper bound ({self._upper_bound}) cannot be lower than lower bound ({self._lower_bound}).'


class _TextLengthFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('length_between')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        lower_bound = int(config['lower_bound'])
        upper_bound = int(config['upper_bound'])
        return _TextLength(column_name, lower_bound, upper_bound)
