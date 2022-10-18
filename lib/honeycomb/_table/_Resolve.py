import textwrap
import datetime
from urllib.parse import urlparse
from typing import Any, List
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import honeycomb.table


class _Resolve(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.resolve')
        self._spark = spark

    def __call__(self, table: str, dst_dir: str, partition_cols: List[str],
                 time_now_dt: datetime.datetime, **kwargs) -> bool:
        return self._resolve(self._spark, table, dst_dir, partition_cols,
                             time_now_dt, **kwargs)

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Resolve structural inconsistencies with the table

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]
             dst_dir: str
                     The table destination directory
             partition_cols: List[str]
                     A list of partition columns
             time_now_dt: datetime.datetime
                     A timestamp used to generate temp tables

             Returns
             -------
             
             bool
                    True if the table was resolved/changed
        """
        print(textwrap.dedent(docstring))

    def _resolve(self, spark: SparkSession, table: str, dst_dir: str,
                 partition_cols: List[str], time_now_dt: datetime.datetime,
                 **kwargs) -> bool:
        changed = False
        location = honeycomb.table.location(spark, f'{table}')
        existing_partition_cols = honeycomb.table.partitions(spark, f'{table}')
        inconsistent_partitions = existing_partition_cols != partition_cols
        inconsistent_location = False
        if dst_dir:
            inconsistent_location = urlparse(location).path != urlparse(
                dst_dir).path
        elif honeycomb.table.is_external(spark, f'{table}'):
            inconsistent_location = True

        if inconsistent_partitions or inconsistent_location:
            changed = True
            if inconsistent_partitions:
                print(
                    f'Resolving partition inconsistencies: existing_partition_cols={existing_partition_cols}, partition_cols={partition_cols}'
                )
            if inconsistent_location:
                print(
                    f'Resolving location inconsistencies: location={location}, dst_dir={dst_dir}'
                )
            suffix = time_now_dt.strftime("%Y_%m_%d_%H_%M_%S")

            tmp_tbl = f'{table}_{suffix}'
            if honeycomb.table.is_delta(spark, f'{table}'):
                print(f'Cloning: {table} to temporary table: {tmp_tbl}')
                spark.sql(f'CREATE OR REPLACE TABLE {tmp_tbl} CLONE {table}')
            else:
                print(f'Copying: {table} to temporary table: {tmp_tbl}')
                spark.table(f'{table}').write.mode('overwrite').format(
                    'parquet').saveAsTable(tmp_tbl)

            print(f'Dropping destination table: {table}')
            spark.sql(
                f"ALTER TABLE {table} SET TBLPROPERTIES('external'='false')")
            honeycomb.table.drop(spark, f'{table}', force=True)

            writer = spark.table(tmp_tbl).write
            if len(partition_cols) > 0:
                writer = writer.partitionBy(partition_cols)
            writer = writer \
                .mode('overwrite') \
                .format('delta')
            if dst_dir:
                writer = writer.option('path', dst_dir)

            writer.saveAsTable(f'{table}')
            partitions = honeycomb.table.partitions(spark, f'{table}')
            print(f'{table}: partitions={partitions}')
            print(f'Dropping temporary table: {tmp_tbl}')
            honeycomb.table.drop(spark, f'{tmp_tbl}', force=True)

        return changed
