import pytest

import honeycomb.strings


def test_strings_split_csv_no_whitespace(spark):
    value = '1,2,3'
    result = honeycomb.strings.split(value)
    assert len(result) == 3


def test_strings_split_colon_no_whitespace(spark):
    value = '1:2:3'
    result = honeycomb.strings.split(value, delimiter=':')
    assert len(result) == 3


def test_strings_split_one(spark):
    value = '1,2,3,4,5'
    result = honeycomb.strings.split(value, num=1)
    assert len(result) == 2


def test_strings_split_pad_length(spark):
    value = '1,2,3'
    result = honeycomb.strings.split(value, num=5)
    assert len(result) == 5


def test_strings_split_csv_whitespace(spark):
    value = '1 ,2 ,    3'
    result = honeycomb.strings.split(value)
    assert len(result) == 3
    assert result == ['1', '2', '3']


def test_strings_split_lowercase(spark):
    value = 'Y ,z ,    W'
    result = honeycomb.strings.split(value, action=honeycomb.strings.lower)
    assert len(result) == 3
    assert result == ['y', 'z', 'w']


def test_strings_split_lowercase_action_none(spark):
    value = 'Y ,z ,    W'
    result = honeycomb.strings.split(value, action=None)
    assert len(result) == 3
    assert result == ['Y', 'z', 'W']


def test_strings_split_uppercase(spark):
    value = 'y ,z ,    W'
    result = honeycomb.strings.split(value, action=lambda x: x.upper())
    assert len(result) == 3
    assert result == ['Y', 'Z', 'W']


def test_strings_split_unpack_exact(spark):
    value = 'issues:fields'
    field1, field2 = honeycomb.strings.split(value, delimiter=':', num=2)
    assert field1 == 'issues'
    assert field2 == 'fields'


def test_strings_split_unpack_missing(spark):
    value = 'issues'
    field1, field2 = honeycomb.strings.split(value, delimiter=':', num=2)
    assert field1 == 'issues'
    assert field2 == None


def test_strings_split_empty(spark):
    value = ''
    result = honeycomb.strings.split(value)
    assert len(result) == 0


def test_strings_split_none(spark):
    value = None
    result = honeycomb.strings.split(value)
    assert result is None


def test_strings_split_dict_no_whitespace(spark):
    value = 'Key1=Value1,Key2=Value2'
    result = honeycomb.strings.split_dict(value)
    assert len(result) == 2
    assert 'Key1' in result
    assert 'Key2' in result
    assert result['Key1'] == 'Value1'
    assert result['Key2'] == 'Value2'


def test_strings_split_dict_whitespace(spark):
    value = 'key1 =    value1 , key2=value2,key3=value3'
    result = honeycomb.strings.split_dict(value)
    assert len(result) == 3
    assert 'key1' in result
    assert 'key2' in result
    assert 'key3' in result
    assert result['key1'] == 'value1'
    assert result['key2'] == 'value2'
    assert result['key3'] == 'value3'


def test_strings_split_dict_lower(spark):
    value = 'Key1=Value1,Key2=Value2'
    result = honeycomb.strings.split_dict(value, action=honeycomb.strings.lower)
    assert len(result) == 2
    assert 'key1' in result
    assert 'key2' in result
    assert result['key1'] == 'value1'
    assert result['key2'] == 'value2'


def test_strings_get_value_valid(spark):
    value = 'value'
    result = honeycomb.strings.get_value(value)
    assert result == value


def test_strings_get_value_empty(spark):
    value = ''
    result = honeycomb.strings.get_value(value)
    assert result is None


def test_strings_get_value_null(spark):
    value = None
    result = honeycomb.strings.get_value(value)
    assert result is None


def test_strings_get_value_null_default(spark):
    value = None
    result = honeycomb.strings.get_value(value, default='foo')
    assert result is 'foo'


def test_strings_get_value_valid_default(spark):
    value = 'value'
    result = honeycomb.strings.get_value(value, 'foo')
    assert result == value


def test_strings_strtobool_true(spark):
    value = 'true'
    result = honeycomb.strings.get_value(value,
                                         default=False,
                                         action=honeycomb.strings.strtobool)
    assert result == True


def test_strings_strtobool_true(spark):
    with pytest.raises(ValueError, match=r".*invalid truth.*"):
        value = 'foo'
        result = honeycomb.strings.get_value(value,
                                             default=False,
                                             action=honeycomb.strings.strtobool)


def test_strings_strtobool_None(spark):
    value = None
    result = honeycomb.strings.get_value(value,
                                         default=False,
                                         action=honeycomb.strings.strtobool)
    assert result == False


def test_strings_strtobool_false(spark):
    value = 'false'
    result = honeycomb.strings.get_value(value,
                                         default=False,
                                         action=honeycomb.strings.strtobool)
    assert result == False
