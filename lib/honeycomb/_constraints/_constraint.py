from typing import List, Tuple, Dict
from abc import ABC, abstractmethod
import random
import string

from pyspark.sql import DataFrame


def _generate_constraint_column_name(constraint_type: str,
                                     column_names: List[str]) -> str:
    random_suffix: str = ''.join(
        random.choice(string.ascii_lowercase) for i in range(12))
    identifier: str = '_'.join(column_names)
    return f"__honeycomb__{identifier}_{constraint_type}_{random_suffix}"


class _Constraint(ABC):

    def __init__(self, constraint_name: str):
        self._constraint_name = constraint_name

    @abstractmethod
    def filter_success(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def filter_failure(self, df: DataFrame) -> DataFrame:
        pass

    def prepare_df_for_check(self, df: DataFrame) -> DataFrame:
        return df

    def validate_self(self, df: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        missing_columns = [c for c in self.column_names if c not in df_columns]
        return len(
            missing_columns
        ) == 0, f"Missing one or more columns: {', '.join(missing_columns)}"

    @property
    def constraint_name(self) -> str:
        return self._constraint_name

    @property
    @abstractmethod
    def column_names(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def constraint_column_name(self) -> str:
        pass

    def __eq__(self, other) -> bool:
        if isinstance(other, _Constraint):
            return self._constraint_name == other._constraint_name
        return False


class _UnaryConstraint(_Constraint, ABC):

    def __init__(self, constraint_name: str, column_name: str):
        super().__init__(constraint_name)
        self._column_name = column_name.lower()
        self._constraint_column_name = _generate_constraint_column_name(
            self.constraint_name, self.column_name)

    @property
    def column_names(self) -> List[str]:
        return [self._column_name]

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    def constraint_column_name(self) -> str:
        return self._constraint_column_name

    def __eq__(self, other) -> bool:
        if isinstance(other, _UnaryConstraint):
            return super().__eq__(
                other) and self._column_name == other._column_name
        return False


class _CompoundConstraint(_Constraint, ABC):

    def __init__(self, constraint_name: str, column_names: List[str]):
        super().__init__(constraint_name)
        self._column_names = [c.lower() for c in column_names]
        self._constraint_column_name = _generate_constraint_column_name(
            self.constraint_name, self.column_names)

    @property
    def column_names(self) -> List[str]:
        return self._column_names

    @property
    def constraint_column_name(self) -> str:
        return self._constraint_column_name

    def __eq__(self, other) -> bool:
        if isinstance(other, _CompoundConstraint):
            return super().__eq__(
                other) and self._column_names == other._column_names
        return False


class _ConstraintFactory(ABC):

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    @abstractmethod
    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        pass
