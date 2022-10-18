from typing import List, Tuple, Dict

from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _ConstraintFactory, _Constraint


class _TextRegex(_UnaryConstraint):

    def __init__(self, column_name: str, regex: str):
        super().__init__('regex_match', column_name)
        self.regex = regex

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.constraint_column_name,
                             df[self.column_name].rlike(self.regex))

    def validate_self(self, df: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        parent_validation_result = super().validate_self(df, df_columns)
        if not parent_validation_result[0]:
            return parent_validation_result
        else:
            column_type = [
                dtype for name, dtype in df.dtypes if name == self.column_name
            ][0]
            return column_type == 'string', f"Column {self.column_name} is not a string."

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.constraint_column_name} = TRUE")

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.constraint_column_name} = FALSE")


class _TextRegexFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('matches')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        regex = config['regex']
        return _TextRegex(column_name, regex)
