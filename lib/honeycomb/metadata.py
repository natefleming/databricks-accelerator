from __future__ import annotations
from abc import ABC, abstractmethod
import os
import logging

from typing import NamedTuple, List, Any

import honeycomb.filesystem
from honeycomb._metadata._Metadata import _Metadata
from honeycomb._metadata._Metadata import _ColumnMetadata
from honeycomb._metadata._Metadata import _DatasetMetadata

LOGGER = logging.getLogger(__name__)

HC_SECURE = 'hc_secure'
HC_MASK = 'hc_masking'
HC_MASK_ROLES = 'hc_masking_roles'
HC_ROW_ACCESS = 'hc_row_access'
HC_ROW_ACCESS_ROLES = 'hc_row_access_roles'
HC_PRIMARY_KEY = 'hc_primary_key'
HC_MODIFICATION_COL = 'hc_modification_col'
HC_DELETE_COL = 'hc_delete_col'
HC_ROW_ACCESS_POLICY_NAME = 'hc_row_access_policy_name'
HC_MASKING_POLICY_NAME = 'hc_masking_policy_name'


class MetadataHandler(ABC):

    @abstractmethod
    def handle_column_metadata(self, metadata: Metadata,
                               metadata_entry: _ColumnMetadata):
        pass

    @abstractmethod
    def handle_dataset_metadata(self, metadata: Metadata,
                                metadata_entry: _DatasetMetadata):
        pass


class MetadataResult(NamedTuple):
    data: DataFrame


class Metadata:
    """
    Adds column metadata to a Spark dataframe schema

    Usage example:
        Metadata(spark_session, data_frame) \
            .add("column_name", 'pii', True) \
            .execute()
    """

    def __init__(self,
                 spark: SparkSession,
                 data_frame: DataFrame,
                 config: dict = None):
        self._spark: SparkSession = spark
        self._df: DataFrame = data_frame
        self._metadata: List[_Metadata] = []
        if config:
            self._load(config)

    def add_column_metadata(self, column_name: str, name: str,
                            value: Any) -> Metadata:
        self._add_metadata(_ColumnMetadata(column_name, name, value))
        return self

    def add_dataset_metadata(self, name: str, value: Any) -> Metadata:
        self._add_metadata(_DatasetMetadata(name, value))
        return self

    def columns(self) -> List[_ColumnMetadata]:
        return [m for m in self._metadata if isinstance(m, _ColumnMetadata)]

    def dataset(self) -> List[_DatasetMetadata]:
        return [m for m in self._metadata if isinstance(m, _DatasetMetadata)]

    def has_column_metadata(self, column_name: str, metadata_name: str) -> bool:
        return any(
            column_name == m.column_name and metadata_name == m.metadata_name()
            for m in self.columns())

    def has_dataset_metadata(self, metadata_name: str) -> bool:
        return any(metadata_name == m.metadata_name() for m in self.dataset())

    def find_dataset_metadata(self,
                              metadata_name: str,
                              default_value=None) -> Any:
        metadata = next(
            filter(lambda m: metadata_name == m.metadata_name(),
                   self.dataset()), None)
        return metadata.metadata_value() if metadata else default_value

    def find_column_metadata(self,
                             column_name: str,
                             metadata_name: str,
                             default_value=None) -> Any:
        metadata = next(
            filter(
                lambda m: column_name == m.column_name and metadata_name == m.
                metadata_name(), self.columns()), None)
        return metadata.metadata_value() if metadata else default_value

    def execute(self) -> MetadataResult:
        self._validate_metadata()

        if self._metadata:
            output = self._df
            for m in self._metadata:
                output = m.apply(output)
            return MetadataResult(output)
        else:
            return MetadataResult(self._df)

    def is_secure(self) -> bool:
        has_secure_metadata = any(
            m.metadata_name().lower() in [HC_SECURE, HC_MASK, HC_ROW_ACCESS]
            for m in self._metadata)
        print(f'is_secure: {has_secure_metadata}')
        return has_secure_metadata

    def handle(self, handler: MetadataHandler) -> None:
        for m in self.columns():
            handler.handle_column_metadata(self, m)
        for m in self.dataset():
            handler.handle_dataset_metadata(self, m)

    def _add_metadata(self, metadata: _Metadata) -> None:
        existing = filter(
            lambda c: c.metadata_name() == metadata.metadata_name() and c.
            column_name == metadata.column_name, self._metadata)
        if list(existing):
            raise ValueError(
                f"An not_null metadata for column {metadata.column_name} already exists."
            )

        self._metadata.append(metadata)

    def _validate_metadata(self) -> None:
        columns = self._df.columns
        errors = []
        for m in self._metadata:
            is_correct, error_message = m.validate_self(self._df, columns)
            if not is_correct:
                errors.append(error_message)

        if errors:
            raise ValueError(", ".join(errors))

    def _load(self, config: dict):
        root = config.get('metadata', {})
        dataset = root.get('dataset', {})
        metadatas = dataset.get('metadata', [])
        for m in metadatas:
            for name, value in m.items():
                self.add_dataset_metadata(name, value)

        columns = root.get('columns', [])
        for column in columns:
            column_name = column.get('column')
            metadatas = column.get('metadata', {})
            if column_name:
                for m in metadatas:
                    for name, value in m.items():
                        self.add_column_metadata(column_name, name, value)
