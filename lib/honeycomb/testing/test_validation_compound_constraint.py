import pytest

from honeycomb._constraints._constraint import _CompoundConstraint


class NullCompoundConstraint(_CompoundConstraint):

    def __init__(self, constraint_name, column_names):
        super().__init__(constraint_name, column_names)

    def filter_success(self, df):
        pass

    def filter_failure(self, df):
        pass


def test_unary_constraint_should_equal():

    constraint_name_lhs: str = 'myconstraint1'
    column_names_lhs: str = ['mycolumn1', 'mycolumn2']

    lhs = NullCompoundConstraint(constraint_name_lhs, column_names_lhs)
    rhs = NullCompoundConstraint(constraint_name_lhs, column_names_lhs)

    assert lhs == rhs


def test_unary_constraint_should_not_equal():

    constraint_name_lhs: str = 'myconstraint1'
    column_names_lhs: str = ['mycolumn1', 'mycolumn2']

    constraint_name_rhs: str = 'myconstraint2'
    column_names_rhs: str = ['mycolumn2', 'mycolumn3']

    lhs = NullCompoundConstraint(constraint_name_lhs, column_names_lhs)
    rhs = NullCompoundConstraint(constraint_name_rhs, column_names_rhs)

    assert lhs != rhs


def test_unary_constraint_should_not_equal_1():

    constraint_name_lhs: str = 'myconstraint1'
    column_names_lhs: str = ['mycolumn1', 'mycolumn2']

    constraint_name_rhs: str = 'myconstraint2'
    column_names_rhs: str = ['mycolumn3', 'mycolumn4']

    lhs = NullCompoundConstraint(constraint_name_lhs, column_names_lhs)
    rhs = NullCompoundConstraint(constraint_name_lhs, column_names_rhs)

    assert lhs != rhs
