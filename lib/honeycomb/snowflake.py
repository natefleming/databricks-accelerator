import re

import honeycomb
import honeycomb.utils
import honeycomb.metadata

from typing import NamedTuple, List, Any, Tuple
from collections import namedtuple

from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection


def engine(spark, **kwargs):
    dbutils = honeycomb.utils.dbutils(spark)
    secret_scope = 'honeycomb-secrets-kv' if 'secret_scope' not in kwargs else kwargs.get(
        'secret_scope')
    snowflake_connection_key = 'ds-snowflake-connection' if 'connection_key' not in kwargs else kwargs.get(
        'connection_key')
    connection_string = dbutils.secrets.get(scope=secret_scope,
                                            key=snowflake_connection_key)

    account = re.search(r'.*//([^?]+)\.snowflakecomputing.*', connection_string)
    account = kwargs.get('account') if 'account' in kwargs else account.group(
        1) if account else None

    user = re.search(r'.*user=([^&]+).*', connection_string)
    user = kwargs.get('user') if 'user' in kwargs else user.group(
        1) if user else None

    password = re.search(r'.*password=([^&]+).*', connection_string)
    password = kwargs.get(
        'password') if 'password' in kwargs else password.group(
            1) if password else None

    database = re.search(r'.*db=([^&]+).*', connection_string)
    database = kwargs.get(
        'database') if 'database' in kwargs else database.group(
            1) if database else None

    schema = re.search(r'.*schema=([^&]+).*', connection_string)
    schema = kwargs.get('schema') if 'schema' in kwargs else schema.group(
        1) if schema else None

    warehouse = re.search(r'.*warehouse=([^&]+).*', connection_string)
    warehouse = kwargs.get(
        'warehouse') if 'warehouse' in kwargs else warehouse.group(
            1) if warehouse else None

    role = re.search(r'.*role=([^&]+).*', connection_string)
    role = kwargs.get('role') if 'role' in kwargs else role.group(
        1) if role else None

    print(f'account:{account}')
    print(f'user:{user}')
    print(f'database:{database}')
    print(f'schema:{schema}')
    print(f'warehouse:{warehouse}')
    print(f'role:{role}')

    engine = create_engine(
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
        ))

    return engine


def is_complex_type(c: T.StructField) -> bool:
    return isinstance(c.dataType, (T.StructType, T.MapType, T.ArrayType))


def resolve_column(col: Column):
    name: str = col.name
    data_type: str = col.dataType.simpleString()
    if is_complex_type(col):
        data_type = 'VARIANT'
    elif isinstance(col.dataType, T.TimestampType):
        data_type = 'TIMESTAMP_TZ'

    return (name, data_type)


SnowflakeProviderContext = namedtuple(
    'SnowflakeProviderContext', 'wh_schema wh_tbl wh_view location dataframe')


class SnowflakeWarehouseProvider(object):

    def __init__(self, connection: Connection):
        self._connection = connection

    def create_external_delta_table(
            self, context: SnowflakeProviderContext,
            metadata: honeycomb.metadata.Metadata) -> None:
        print(
            f'create_external_delta_table(table_schema: {context.wh_schema}, table_name: {context.wh_tbl}, location: {context.location})'
        )
        table_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_tbl] if x])
        manifest_table_name = f'{table_name}_manifest'
        manifest_location = f'{context.location}/_symlink_format_manifest/'
        print(f'manifest_location: {manifest_location}')

        create_manifest_table_sql = f'''
            CREATE OR REPLACE EXTERNAL TABLE {manifest_table_name}(
            filename VARCHAR AS split_part(VALUE:c1, '/', -1)
            )
            WITH LOCATION = {manifest_location}
            FILE_FORMAT = (TYPE = CSV)
            PATTERN = '.*[/]manifest'
            AUTO_REFRESH = true;
        '''
        print(f'create_manifest_table_sql={create_manifest_table_sql}')
        self._connection.execute(create_manifest_table_sql)

        drop_table_sql = f'DROP TABLE IF EXISTS {table_name}'
        print(f'drop_table_sql={drop_table_sql}')
        self._connection.execute(drop_table_sql)

        trusted_schema = [
            honeycomb.snowflake.resolve_column(c)
            for c in context.dataframe.schema
        ]
        trusted_columns = ','.join([
            f'{c[0]} {c[1]} AS (VALUE:{c[0]}::{c[1]})' for c in trusted_schema
        ])

        create_table_sql = f'''
            CREATE OR REPLACE EXTERNAL TABLE {table_name} (
            {trusted_columns},
            PARQUET_FILENAME VARCHAR(16777216) AS (SPLIT_PART(METADATA$FILENAME, '/', -1)))
            location={context.location}/
            auto_refresh=false
            pattern='.*[/]part-[^/]*[.]parquet'
            file_format=(TYPE=PARQUET NULL_IF=())
        '''
        print(f'create_table_sql={create_table_sql}')

        self._connection.execute(create_table_sql)

    def create_external_delta_view(
            self, context: SnowflakeProviderContext,
            metadata: honeycomb.metadata.Metadata) -> None:
        print(
            f'create_external_delta_view(view_schema: {context.wh_schema}, view_name: {context.wh_view}, table_schema: {context.wh_schema}, table_name: {context.wh_tbl})'
        )

        view_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_view] if x])
        table_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_tbl] if x])

        view_columns: str = ','.join(context.dataframe.columns)

        create_view_sql = f'''
            CREATE OR REPLACE VIEW {view_name} AS SELECT {view_columns}
            FROM {table_name}
            WHERE parquet_filename IN (SELECT filename FROM {table_name}_manifest)
        '''
        print(f'create_view_sql={create_view_sql}')

        self._connection.execute(create_view_sql)

    def create_managed_table(self, context: SnowflakeProviderContext,
                             metadata: honeycomb.metadata.Metadata) -> None:
        print(
            f'create_managed_table(table_schema: {context.wh_schema}, table_name: {context.wh_tbl})'
        )
        table_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_tbl] if x])

        drop_manifest_table_sql = f'DROP TABLE IF EXISTS {table_name}_MANIFEST'
        print(f'drop_manifest_table_sql={drop_manifest_table_sql}')
        self._connection.execute(drop_manifest_table_sql)

        if self.is_external(context.wh_schema, context.wh_tbl):
            drop_table_sql = f'DROP TABLE IF EXISTS {table_name}'
            self._connection.execute(drop_table_sql)

        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            JSON_CONTENT VARIANT
        );
        '''
        print(f'create_table_sql={create_table_sql}')
        self._connection.execute(create_table_sql)

    def create_managed_view(self, context: SnowflakeProviderContext,
                            metadata: honeycomb.metadata.Metadata) -> None:
        print(
            f'create_managed_view(view_schema: {context.wh_schema}, view_name: {context.wh_view}, table_schema: {context.wh_schema}, table_name: {context.wh_tbl})'
        )
        view_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_view] if x])
        table_name = '.'.join(
            [x for x in [context.wh_schema, context.wh_tbl] if x])
        trusted_schema = [
            honeycomb.snowflake.resolve_column(c)
            for c in context.dataframe.schema
        ]
        trusted_columns = ', '.join(
            [f'JSON_CONTENT:{c[0]}::{c[1]} AS {c[0]}' for c in trusted_schema])
        cols = '({})'.format(', '.join(context.dataframe.schema.names))

        create_view_sql = f'''
        CREATE OR REPLACE VIEW {view_name} {cols} 
        AS SELECT 
            {trusted_columns}
        FROM {table_name}
        '''

        print(f'create_view_sql={create_view_sql}')
        self._connection.execute(create_view_sql)

    def is_external(self, wh_schema: str, wh_tbl: str) -> bool:
        print(f'is_external(wh_schema: {wh_schema}, wh_tbl: {wh_tbl})')
        wh_schema = wh_schema.upper()
        wh_tbl = wh_tbl.upper()
        is_external_sql: str = f"""
            select exists(
                select 1 from information_schema.tables where table_schema = '{wh_schema}' and table_name = '{wh_tbl}' and table_type = 'EXTERNAL TABLE')
            """
        print(f"is_external_sql: {is_external_sql}")
        exists = self._connection.execute(is_external_sql).fetchone()[0]
        print(f'is_external: {exists}')
        return exists

    def view_exists(self, view_schema: str, view_name: str) -> bool:
        print(
            f'view_exists(view_schema: {view_schema}, view_name: {view_name})')
        view_schema = view_schema.upper()
        view_name = view_name.upper()
        view_exists_sql: str = f"""
            select exists(select 1 from information_schema.views where table_schema = '{view_schema}' and table_name = '{view_name}')
        """
        print(f"view_exists_sql: {view_exists_sql}")
        exists = self._connection.execute(view_exists_sql).fetchone()[0]
        print(f'view_exists: {exists}')
        return exists

    def masking_policy_exists(self, policy_schema: str,
                              policy_name: str) -> bool:
        print(
            f'masking_policy_exists(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )

        policy_schema = policy_schema.upper()
        policy_name = policy_name.upper()

        self._connection.execute('show masking policies')
        masking_policy_exists_sql: str = f"""
            select exists(
            select 1 from(
                select 
                $1 as created_on
                ,$2 as name
                ,$3 as database_name
                ,$4 as schema_name
                ,$5 as kind
                ,$6 as owner
                from table(result_scan(last_query_id())) 
            )
            where kind = 'MASKING_POLICY' and schema_name = '{policy_schema}' and name = '{policy_name}'
            )
        """
        print(f"masking_policy_exists_sql: {masking_policy_exists_sql}")

        exists = self._connection.execute(
            masking_policy_exists_sql).fetchone()[0]
        print(f'masking_policy_exists: {exists}')

        return exists

    def row_access_policy_exists(self, policy_schema: str,
                                 policy_name: str) -> bool:
        print(
            f'row_access_policy_exists(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )

        policy_schema = policy_schema.upper()
        policy_name = policy_name.upper()

        self._connection.execute('show row access policies')
        row_access_policy_exists_sql: str = f"""
            select exists(
            select 1 from(
                select 
                $1 as created_on
                ,$2 as name
                ,$3 as database_name
                ,$4 as schema_name
                ,$5 as kind
                ,$6 as owner
                from table(result_scan(last_query_id())) 
            )
            where kind = 'ROW_ACCESS_POLICY' and schema_name = '{policy_schema}' and name = '{policy_name}'
            )
        """
        print(f"row_access_policy_exists_sql: {row_access_policy_exists_sql}")

        exists = self._connection.execute(
            row_access_policy_exists_sql).fetchone()[0]
        print(f'row_access_policy_exists: {exists}')

        return exists

    def is_row_access_policy_attached(self, policy_schema: str,
                                      policy_name: str, view_schema: str,
                                      view_name: str) -> bool:
        print(
            f'is_row_access_policy_attached(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name})'
        )
        if not self.view_exists(view_schema, view_name):
            return False

        view_name: str = f'{view_schema.upper()}.{view_name.upper()}'
        policy_schema = policy_schema.upper()
        policy_name = policy_name.upper()
        is_row_access_policy_attached_sql: str = f"""
            select exists(
            select 1 
            from table(information_schema.policy_references(ref_entity_name => '{view_name}', ref_entity_domain => 'view')) 
            where policy_kind = 'ROW_ACCESS_POLICY' and policy_schema = '{policy_schema}' and policy_name = '{policy_name}'
            );
        """
        print(
            f"is_row_access_policy_attached_sql: {is_row_access_policy_attached_sql}"
        )
        exists = self._connection.execute(
            is_row_access_policy_attached_sql).fetchone()[0]
        print(f'is_row_access_policy_attached: {exists}')
        return exists

    def is_masking_policy_attached(self, policy_schema: str, policy_name: str,
                                   view_schema: str, view_name: str,
                                   view_column: str) -> bool:
        print(
            f'is_masking_policy_attached(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name}, view_column: {view_column})'
        )
        if not self.view_exists(view_schema, view_name):
            return False

        view_name: str = f'{view_schema.upper()}.{view_name.upper()}'
        policy_schema = policy_schema.upper()
        policy_name = policy_name.upper()
        view_column = view_column.upper()
        is_masking_policy_attached_sql: str = f"""
            select exists(
            select 1 
            from table(information_schema.policy_references(ref_entity_name => '{view_name}', ref_entity_domain => 'view')) 
            where policy_kind = 'MASKING_POLICY' and policy_schema = '{policy_schema}' and policy_name = '{policy_name}' and ref_column_name = '{view_column}'
            );
        """
        print(
            f"is_masking_policy_attached_sql: {is_masking_policy_attached_sql}")
        exists = self._connection.execute(
            is_masking_policy_attached_sql).fetchone()[0]
        print(f'is_masking_policy_attached: {exists}')
        return exists

    def find_row_access_policy_attached(
            self, policy_schema: str,
            policy_name: str) -> List[Tuple[str, str, str]]:
        print(
            f'find_row_access_policy_attached(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )
        if not self.row_access_policy_exists(policy_schema, policy_name):
            return []

        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        row_access_policy_attached_sql: str = f"""
            select 
                ref_database_name, ref_schema_name, ref_entity_name
            from table(information_schema.policy_references(policy_name => '{policy_name}')) where policy_kind = 'ROW_ACCESS_POLICY'
        """
        print(
            f"row_access_policy_attached_sql: {row_access_policy_attached_sql}")
        result_set = self._connection.execute(
            row_access_policy_attached_sql).fetchall()
        print(f'row_access_policy_attached: {result_set}')
        return result_set

    def find_masking_policy_attached(
            self, policy_schema: str,
            policy_name: str) -> List[Tuple[str, str, str, str]]:
        print(
            f'find_masking_policy_attached(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )
        if not self.masking_policy_exists(policy_schema, policy_name):
            return []
        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        masking_policy_attached_sql: str = f"""
            select 
                ref_database_name, ref_schema_name, ref_entity_name, ref_column_name
            from table(information_schema.policy_references(policy_name => '{policy_name}')) where policy_kind = 'MASKING_POLICY'
        """
        print(f"masking_policy_attached_sql: {masking_policy_attached_sql}")
        result_set = self._connection.execute(
            masking_policy_attached_sql).fetchall()
        print(f'find_masking_policy_attached: {result_set}')
        return result_set

    def unset_masking_policy(self, policy_schema: str, policy_name: str,
                             view_schema: str, view_name: str,
                             view_column: str) -> None:
        print(
            f'unset_masking_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name}, view_column: {view_column})'
        )
        if not self.is_masking_policy_attached(policy_schema, policy_name,
                                               view_schema, view_name,
                                               view_column):
            return
        view_name: str = f'{view_schema}.{view_name}'
        unset_masking_policy_sql = f'ALTER VIEW {view_name} MODIFY COLUMN {view_column} UNSET MASKING POLICY;'
        print(f'unset_masking_policy_sql={unset_masking_policy_sql}')
        self._connection.execute(unset_masking_policy_sql)

    def unset_masking_policies(self, policy_schema: str,
                               policy_name: str) -> None:
        print(
            f'unset_masking_policies(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )
        attached = self.find_masking_policy_attached(policy_schema, policy_name)
        for ref_database_name, ref_schema_name, ref_entity_name, ref_column_name in attached:
            self.unset_masking_policy(policy_schema, policy_name,
                                      ref_schema_name, ref_entity_name,
                                      ref_column_name)

    def unset_row_access_policy(self, policy_schema: str, policy_name: str,
                                view_schema: str, view_name: str):
        print(
            f'unset_row_access_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name})'
        )
        if not self.is_row_access_policy_attached(policy_schema, policy_name,
                                                  view_schema, view_name):
            return
        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        view_name: str = f'{view_schema}.{view_name}'
        unset_row_access_policy_sql = f'ALTER TABLE IF EXISTS {view_name} DROP ROW ACCESS POLICY {policy_name}'
        print(f'unset_row_access_policy_sql={unset_row_access_policy_sql}')
        self._connection.execute(unset_row_access_policy_sql)

    def unset_row_access_policies(self, policy_schema: str,
                                  policy_name: str) -> None:
        print(
            f'unset_row_access_policies(policy_schema: {policy_schema}, policy_name: {policy_name})'
        )
        attached = self.find_row_access_policy_attached(policy_schema,
                                                        policy_name)
        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        for ref_database_name, ref_schema_name, ref_entity_name in attached:
            self.unset_row_access_policy(policy_schema, policy_name,
                                         ref_schema_name, ref_entity_name)

    def create_masking_policy(self, policy_schema: str, policy_name: str,
                              masking_policy_roles: List[str], view_schema: str,
                              view_name: str, column: Column) -> None:
        print(
            f'create_masking_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name}, column: {column})'
        )

        view_schema = view_schema.upper()
        view_name = view_name.upper()

        self.unset_masking_policies(policy_schema, policy_name)
        mask_policy: str = f'{policy_schema}.{policy_name}'
        view_column, data_type = honeycomb.snowflake.resolve_column(column)
        roles = ', '.join([f"'{role}'" for role in masking_policy_roles])
        create_masking_policy_sql = f"""
        CREATE OR REPLACE MASKING POLICY {mask_policy} AS (val {data_type}) RETURNS {data_type} ->
            CASE
            WHEN
                current_role() in ({roles})
            THEN val
            WHEN EXISTS(
                SELECT 1 
                FROM TRUSTED_SECURE.WHITE_LIST
                WHERE 
                    upper(policy) = 'COLUMN'
                    and upper(role) = current_role() 
                    and coalesce(upper(schema_name), '{view_schema}') = '{view_schema}'
                    and coalesce(upper(table_name), '{view_name}') = '{view_name}'
                    and coalesce(upper(column_name), '{column.name.upper()}') = '{column.name.upper()}'
            ) 
            THEN val
            ELSE CAST(HASH(val) AS {data_type})
            END;
        """
        print(f'create_masking_policy_sql={create_masking_policy_sql}')
        self._connection.execute(create_masking_policy_sql)

    def attach_masking_policy(self, policy_schema: str, policy_name: str,
                              view_schema: str, view_name: str,
                              view_column: str) -> None:
        print(
            f'attach_masking_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name}, view_column: {view_column})'
        )
        self.unset_masking_policy(policy_schema, policy_name, view_schema,
                                  view_name, view_column)
        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        view_name: str = f'{view_schema}.{view_name}'
        attach_masking_policy_sql = f"""
            ALTER VIEW {view_name} MODIFY COLUMN {view_column} SET MASKING POLICY {policy_name}
        """
        print(f'attach_masking_policy_sql: {attach_masking_policy_sql}')
        self._connection.execute(attach_masking_policy_sql)

    def create_row_access_policy(self, policy_schema: str, policy_name: str,
                                 hc_row_access_roles: List[str], wh_schema: str,
                                 wh_tbl: str, src_df: DataFrame,
                                 hc_row_access: dict) -> None:
        print(
            f'create_row_access_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, src_df: {src_df}, hc_row_access: {hc_row_access}, hc_row_access_roles: {hc_row_access_roles})'
        )

        wh_schema = wh_schema.upper()
        wh_tbl = wh_tbl.upper()

        self.unset_row_access_policies(policy_schema, policy_name)
        access_policy: str = f'{policy_schema}.{policy_name}'

        hc_row_access_columns = hc_row_access.get('columns', [])

        hc_row_access_column_signature = []
        exists_conditions = []
        for hc_row_access_column in hc_row_access_columns:
            hc_row_access_column_name = hc_row_access_column.get('column')
            print(f'hc_row_access_column: {hc_row_access_column}')
            hc_row_access_column_name_data_type = honeycomb.snowflake.resolve_column(
                src_df.schema[hc_row_access_column_name])
            hc_row_access_column_signature += [
                hc_row_access_column_name_data_type
            ]
            hc_row_access_operator = hc_row_access_column.get('operator', '=')
            print(f'hc_row_access_operator: {hc_row_access_operator}')

            hc_row_access_column_name_signature = '({})'.format(', '.join(
                [f'{c[0]}' for c in hc_row_access_column_signature]))

            hc_row_access_condition = f'cast({hc_row_access_column_name} as string) {hc_row_access_operator} column_value'
            hc_row_access_condition = f'((column_value is null) OR ({hc_row_access_condition}))'
            print(
                f'hc_row_access_column_name_signature: {hc_row_access_column_name_signature}'
            )
            print(f'hc_row_access_condition: {hc_row_access_condition}')

            exists_condition = f"""
                EXISTS(
                    SELECT 1 FROM TRUSTED_SECURE.WHITE_LIST
                    WHERE 
                        upper(policy) = 'ROW'
                        and upper(role) = current_role() 
                        and coalesce(upper(schema_name), '{wh_schema}') = '{wh_schema}'
                        and coalesce(upper(table_name), '{wh_tbl}') = '{wh_tbl}'
                        and coalesce(upper(column_name), '{hc_row_access_column_name.upper()}') = '{hc_row_access_column_name.upper()}'
                        and {hc_row_access_condition}
                )
            """
            exists_conditions += [exists_condition]

        row_access_predicate = ' AND '.join(
            exists_conditions) if exists_conditions else 'TRUE'
        hc_row_access_column_type_signature = '({})'.format(', '.join(
            [f'{c[0]} {c[1]}' for c in hc_row_access_column_signature]))

        print(
            f'hc_row_access_column_type_signature: {hc_row_access_column_type_signature}'
        )

        roles = ', '.join([f"'{role}'" for role in hc_row_access_roles])
        create_access_policy_sql = f'''
            CREATE OR REPLACE ROW ACCESS POLICY {access_policy} as {hc_row_access_column_type_signature} RETURNS BOOLEAN ->
            CASE
                WHEN 
                    current_role() in ({roles}) 
                THEN true
                ELSE 
                   {row_access_predicate}
            END
        '''
        print(f'create_access_policy_sql={create_access_policy_sql}')
        self._connection.execute(create_access_policy_sql)

    def attach_row_access_policy(self, policy_schema: str, policy_name: str,
                                 view_schema: str, view_name: str,
                                 hc_row_access: dict) -> None:
        print(
            f'attach_row_access_policy(policy_schema: {policy_schema}, policy_name: {policy_name}, view_schema: {view_schema}, view_name: {view_name}, hc_row_access: {hc_row_access})'
        )
        self.unset_row_access_policy(policy_schema, policy_name, view_schema,
                                     view_name)
        policy_name: str = f'{policy_schema.upper()}.{policy_name.upper()}'
        view_name: str = f'{view_schema}.{view_name}'
        column_names = [
            c.get('column')
            for c in hc_row_access.get('columns', [])
            if 'column' in c
        ]
        columns = ', '.join(column_names)
        attach_row_access_policy_sql = f"""
            ALTER VIEW IF EXISTS {view_name} ADD ROW ACCESS POLICY {policy_name} ON ({columns}) 
        """
        print(f'attach_row_access_policy_sql: {attach_row_access_policy_sql}')
        self._connection.execute(attach_row_access_policy_sql)


class SnowflakeMetadataHandler(honeycomb.metadata.MetadataHandler):

    def __init__(self, provider: SnowflakeWarehouseProvider,
                 context: SnowflakeProviderContext):
        self._provider = provider
        self._context = context

    @property
    def provider(self) -> SnowflakeWarehouseProvider:
        return self._provider

    @property
    def context(self) -> SnowflakeProviderContext:
        return self._context

    def handle_column_metadata(
            self, metadata: honeycomb.metadata.Metadata,
            metadata_entry: honeycomb.metadata._ColumnMetadata):
        print(
            f'handle_column_metadata(metadata: {metadata}, metadata_entry: {metadata_entry})'
        )
        has_mask: bool = metadata_entry.metadata_name().lower() in [
            honeycomb.metadata.HC_SECURE, honeycomb.metadata.HC_MASK
        ]
        if has_mask:
            self._handle_column_masking(metadata, metadata_entry)

    def handle_dataset_metadata(
            self, metadata: honeycomb.metadata.Metadata,
            metadata_entry: honeycomb.metadata._DatasetMetadata):
        print(
            f'handle_dataset_metadata(metadata: {metadata}, metadata_entry: {metadata_entry})'
        )
        has_row_access: bool = metadata_entry.metadata_name().lower() in [
            honeycomb.metadata.HC_ROW_ACCESS
        ]
        if has_row_access:
            self._handle_dataset_row_access(metadata, metadata_entry)

    def _get_masking_roles(self, name: str,
                           metadata: honeycomb.metadata.Metadata) -> List[str]:
        masking_policy_roles = metadata.find_column_metadata(
            name, honeycomb.metadata.HC_MASK_ROLES, [])
        masking_policy_roles += ['ACCOUNTADMIN']
        masking_policy_roles = list(
            set([r.upper() for r in masking_policy_roles]))
        return masking_policy_roles

    def _get_row_access_roles(
            self, metadata: honeycomb.metadata.Metadata) -> List[str]:
        row_access_policy_roles = metadata.find_dataset_metadata(
            honeycomb.metadata.HC_ROW_ACCESS_ROLES, [])
        row_access_policy_roles += ['ACCOUNTADMIN']
        row_access_policy_roles = list(
            set([r.upper() for r in row_access_policy_roles]))
        return row_access_policy_roles

    def _get_role_access_policy_name(
            self, metadata: honeycomb.metadata.Metadata) -> str:
        row_access_policy_name = metadata.find_dataset_metadata(
            honeycomb.metadata.HC_ROW_ACCESS_POLICY_NAME,
            f'{self.context.wh_view}_ACCESS')
        return row_access_policy_name

    def _handle_dataset_row_access(
            self, metadata: honeycomb.metadata.Metadata,
            metadata_entry: honeycomb.metadata._DatasetMetadata):
        print(f'_handle_dataset_row_access(metadata: {metadata})')
        hc_row_access = metadata_entry.metadata_value()
        row_access_policy_name = self._get_role_access_policy_name(metadata)
        row_access_policy_roles = self._get_row_access_roles(metadata)
        self.provider.create_row_access_policy(
            self.context.wh_schema, row_access_policy_name,
            row_access_policy_roles, self.context.wh_schema,
            self.context.wh_view, self.context.dataframe, hc_row_access)
        self.provider.attach_row_access_policy(self.context.wh_schema,
                                               row_access_policy_name,
                                               self.context.wh_schema,
                                               self.context.wh_view,
                                               hc_row_access)

    def _handle_column_masking(
            self, metadata: honeycomb.metadata.Metadata,
            metadata_entry: honeycomb.metadata._ColumnMetadata):
        print(f'_handle_column_masking(metadata: {metadata})')
        name = metadata_entry.column_name
        column_masking_policy_name = metadata.find_column_metadata(
            name, honeycomb.metadata.HC_MASKING_POLICY_NAME,
            f'{self.context.wh_view}_{name}_MASK')
        masking_policy_roles = self._get_masking_roles(name, metadata)
        self.provider.create_masking_policy(self.context.wh_schema,
                                            column_masking_policy_name,
                                            masking_policy_roles,
                                            self.context.wh_schema,
                                            self.context.wh_view,
                                            self.context.dataframe.schema[name])
        self.provider.attach_masking_policy(self.context.wh_schema,
                                            column_masking_policy_name,
                                            self.context.wh_schema,
                                            self.context.wh_view, name)
