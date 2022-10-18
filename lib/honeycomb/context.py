import honeycomb.utils

from typing import NamedTuple
from pyspark.sql import SparkSession

from abc import ABC, abstractmethod


class Salt(NamedTuple):
    value: str
    version: str


class ContextBase(ABC):

    @property
    @abstractmethod
    def salt(self) -> Salt:
        pass

    @property
    @abstractmethod
    def connection_string(self) -> str:
        pass


class NullContext(ContextBase):

    @property
    def salt(self) -> Salt:
        return None

    @property
    def connection_string(self) -> str:
        return None


class Context(ContextBase):

    def __init__(self, spark: SparkSession, **kwargs):
        self.dbutils = honeycomb.utils.dbutils(spark)
        self.secret_scope = 'honeycomb-secrets-kv' if 'secret_scope' not in kwargs else kwargs.get(
            'secret_scope')
        self.connection_key = 'ds-mssql-connection' if 'connection_key' not in kwargs else kwargs.get(
            'connection_key')
        self.salt_key = 'salt-value' if 'salt_key' not in kwargs else kwargs.get(
            'salt_key')
        self.salt_version = 'salt-version' if 'salt_version' not in kwargs else kwargs.get(
            'salt_version')

    @property
    def salt(self) -> Salt:
        salt: Salt = None
        if self._secret_exists(self.secret_scope, self.salt_key):
            salt_value = self.dbutils.secrets.get(scope=self.secret_scope,
                                                  key=self.salt_key)
            salt_version: str = None
            if self._secret_exists(self.secret_scope, self.salt_version):
                salt_version = self.dbutils.secrets.get(scope=self.secret_scope,
                                                        key=self.salt_version)
            salt = Salt(salt_value, salt_version)
        return salt

    @property
    def connection_string(self) -> str:
        value = self.dbutils.secrets.get(scope=self.secret_scope,
                                         key=self.connection_key)
        return value

    def _secret_exists(self, secret_scope: str, key: str) -> bool:
        return any([
            metadata.key == key
            for metadata in self.dbutils.secrets.list(secret_scope)
        ])
