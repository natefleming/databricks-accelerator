import pyspark.sql.functions as F
import json


@F.udf('string')
def append_tag(tag_col, key, value):
    d = json.loads(tag_col)
    d[key] = value
    return json.dumps(d)
