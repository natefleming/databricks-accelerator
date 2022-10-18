import pytest
import honeycomb.utils
from honeycomb._transformations._transformation import _Transformation
from honeycomb._constraints._constraint import _Constraint


def test_find_transformation_classes():

    classes = honeycomb.utils.find_members(
        'honeycomb._transformations',
        honeycomb.utils.is_child_of(_Transformation))
    print(classes)
    assert len(classes) > 0
    assert all(issubclass(c, _Transformation) for c in classes)


def test_find_contraint_classes():

    classes = honeycomb.utils.find_members(
        'honeycomb._constraints', honeycomb.utils.is_child_of(_Constraint))
    print(classes)
    assert len(classes) > 0
    assert all(issubclass(c, _Constraint) for c in classes)
