import pytest

from honeycomb._constraints._constraint import _UnaryConstraint


class NullUnaryConstraint(_UnaryConstraint):

    def __init__(self, constraint_name, column_name):
        super().__init__(constraint_name, column_name)

    def filter_success(self, df):
        pass

    def filter_failure(self, df):
        pass


def test_unary_constraint_should_equal():

    constraint_name_lhs: str = 'myconstraint1'
    column_name_lhs: str = 'mycolumn1'

    lhs = NullUnaryConstraint(constraint_name_lhs, column_name_lhs)
    rhs = NullUnaryConstraint(constraint_name_lhs, column_name_lhs)

    assert lhs == rhs


def test_unary_constraint_should_not_equal():

    constraint_name_lhs: str = 'myconstraint1'
    column_name_lhs: str = 'mycolumn1'

    constraint_name_rhs: str = 'myconstraint2'
    column_name_rhs: str = 'mycolumn2'

    lhs = NullUnaryConstraint(constraint_name_lhs, column_name_lhs)
    rhs = NullUnaryConstraint(constraint_name_rhs, column_name_rhs)

    assert lhs != rhs


def test_unary_constraint_should_not_equal_1():

    constraint_name_lhs: str = 'myconstraint1'
    column_name_lhs: str = 'mycolumn1'

    constraint_name_rhs: str = 'myconstraint2'
    column_name_rhs: str = 'mycolumn2'

    lhs = NullUnaryConstraint(constraint_name_lhs, column_name_lhs)
    rhs = NullUnaryConstraint(constraint_name_lhs, column_name_rhs)

    assert lhs != rhs
