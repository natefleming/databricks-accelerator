import pytest
import honeycomb.utils


def test_retry(spark):
    count = [1]
    expected_count = 3

    def fail_call(count, expected_count):
        print(f'fail_call(count={count}, expected_count={expected_count})')
        count[0] += 1
        if count[0] < expected_count:
            raise Exception(f'Exception: {count}')

    honeycomb.utils.retry_call(fail_call,
                               fargs=[count, expected_count],
                               tries=expected_count)

    assert count[0] == expected_count


def test_lambda_retry(spark):
    count = [1]
    expected_count = 3

    def fail_call(count, expected_count):
        print(f'fail_call(count={count}, expected_count={expected_count})')
        count[0] += 1
        if count[0] < expected_count:
            raise Exception(f'Exception: {count}')

    honeycomb.utils.retry_call(lambda: fail_call(count, expected_count),
                               tries=expected_count)

    assert count[0] == expected_count


def test_lambda_retry_with_delay(spark):
    count = [1]
    expected_count = 3

    def fail_call(count, expected_count):
        print(f'fail_call(count={count}, expected_count={expected_count})')
        count[0] += 1
        if count[0] < expected_count:
            raise Exception(f'Exception: {count}')

    honeycomb.utils.retry_call(lambda: fail_call(count, expected_count),
                               tries=expected_count,
                               delay=5)

    assert count[0] == expected_count
