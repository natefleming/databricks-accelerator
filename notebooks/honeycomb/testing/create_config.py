# Databricks notebook source
data = r'''
---
metadata:
  columns:
  - column: ssn
    metadata:
    - hc_secure: 'true'
transformation:
  transformations:
  - name: skip_empty_rows
  columns:
  - column: age
    transformations:
    - name: min_max
      lower_bound: -1
      upper_bound: 1
      output_column: age_out
validation:
  columns:
  - column: id
    constraints:
    - name: is_not_null
    - name: is_primary_key
  - column: first_name
    constraints:
    - name: is_not_null
    - name: is_unique
    - name: has_length_between
      lower_bound: '0'
      upper_bound: '10'
    - name: text_matches_regex
      regex: "^[a-z]{3,10}$"
  - column: last_name
    constraints:
    - name: is_not_null
    - name: is_unique
    - name: one_of
      values:
      - 1
      - 2
      - 3
  - column: age
    constraints:
    - name: is_min
      value: 10
    - name: is_max
      value: 20
    - name: in_between
      lower_bound: 10
      upper_bound: 20
    - name: one_of
      values:
      - 1
      - 2
      - 3
      - 4
      - 5
'''

path = '/tmp/test.yml'
dbutils.fs.put(path, data, overwrite=True)
path = f'/dbfs/{path}'
