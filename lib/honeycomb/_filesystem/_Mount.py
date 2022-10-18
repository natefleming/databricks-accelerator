import textwrap
import re
import logging
from typing import Any, Tuple

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

MountInfo = Any

LOGGER = logging.getLogger(__name__)


class _Mount(_Command):

    def __init__(self, spark: SparkSession, client: str = None):
        super().__init__('honeycomb.filesystem.mount')
        self._spark = spark
        self._client = client

    def __call__(self, container: str, **kwargs) -> Tuple[MountInfo, Exception]:
        client = kwargs.get('client') if 'client' in kwargs else self._client
        storage_account = re.sub('[^\\w]+', '',
                                 '{0}adlg2'.format(client)) if client else None
        storage_account = kwargs.get('storage_account', storage_account)
        secret_scope = 'honeycomb-secrets-kv' if 'secret_scope' not in kwargs else kwargs.get(
            'secret_scope')
        secret_key = 'rs-data-lake-account-key' if 'secret_key' not in kwargs else kwargs.get(
            'secret_key')
        if not storage_account:
            raise ValueError(
                "Missing argument: client='string' or storage_account='string'")
        mount_point = kwargs.get('mount_point', '/mnt/{0}'.format(container))

        LOGGER.info(f'client={client}')
        LOGGER.info(f'storage_account={storage_account}')
        LOGGER.info(f'secret_scope={secret_scope}')
        LOGGER.info(f'secret_key={secret_key}')
        LOGGER.info(f'mount_point={mount_point}')

        try:
            result = self._mount(self._spark, storage_account, mount_point,
                                 container, secret_scope, secret_key)
            return (result, None)
        except Exception as e:
            return (None, e)

    def help(self):
        docstring = f"""
             {self.name}(container: str)

             Mount an ADLS container

             Parameters
             ----------

             container : str
                         The ADLS container to be mounted. default: None

             client : str, optional
                      The client name

             storage_account : str, optional
                               The Azure storage account name. default: [client]adlg2

             secret_key : str, optional
                          The Azure keyvault secret name for the Azure storage account key. default: rs-data-lake-account-key

             mount_point : str, optional
                           The DBFS location which the container will be mounted. default: /mnt/[container]

             Returns
             -------
             
             Tuple[dbutils.MountInfo, Exception] 

                                 A tuple containing n object representing the mounted container and any raised exceptions
                                

            Examples
            --------

            Mount the ADLS container at DBFS location:  /mnt/trusted
            >>> honeycomb.filesystem.mount('trusted')
            MountInfo(mountPoint='/mnt/trusted', source='wasbs://trusted@[client]adlg2.blob.core.windows.net', encryptionType='')

            Mount the ADLS container at DBFS location:  /mnt/trusted
            >>> honeycomb.filesystem.mount(mount_point='/mnt/trusted')
            MountInfo(mountPoint='/mnt/trusted', source='wasbs://trusted@[client]adlg2.blob.core.windows.net', encryptionType='')

            Mount the ADLS container at DBFS location:  /mnt/trusted using storage account: fooadlg2
            >>> honeycomb.filesystem.mount(client='foo', mount_point='/mnt/trusted')
            MountInfo(mountPoint='/mnt/trusted', source='wasbs://trusted@fooadlg2.blob.core.windows.net', encryptionType='')

            Mount the ADLS container at DBFS location:  /mnt/trusted using storage account: mystorageaccount and the secret stored with mysecretkey
            >>> honeycomb.filesystem.mount('trusted', storage_account='mystorageaccount', secret_key='mysecretkey')
            MountInfo(mountPoint='/mnt/trusted', source='wasbs://trusted@mystorageaccount.blob.core.windows.net', encryptionType='')

        """
        print(textwrap.dedent(docstring))

    def _mount(self, spark: SparkSession, storage_account: str,
               mount_point: str, container: str, secret_scope: str,
               secret_key: str):
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        mounted = next(
            iter([
                m for m in dbutils_wrapper.fs.mounts()
                if m.mountPoint == mount_point
            ]), None)
        if mounted:
            mounted_source = re.search('.*@([^.]+).*', mounted.source)
            if mounted_source and storage_account != mounted_source.group(1):
                dbutils_wrapper.fs.unmount(mount_point)
                mounted = None
        if not mounted:
            source = 'wasbs://{container}@{storage_account}.blob.core.windows.net'.format(
                container=container, storage_account=storage_account)
            account_key = 'fs.azure.account.key.{storage_account}.blob.core.windows.net'.format(
                storage_account=storage_account)
            try:
                result = dbutils_wrapper.fs.mount(
                    source=source,
                    mount_point=mount_point,
                    extra_configs={
                        account_key:
                            dbutils_wrapper.secrets.get(secret_scope,
                                                        secret_key)
                    })
            except BaseException as e:
                if 'Directory already mounted' not in str(e):
                    LOGGER.error(str(e))
                    raise e
                else:
                    LOGGER.warn(f'Directory already mounted: {mount_point}')
        result = next(
            iter([
                m for m in dbutils_wrapper.fs.mounts()
                if m.mountPoint == mount_point
            ]), None)
        return result
