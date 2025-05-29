from collections.abc import Iterable
from typing import Literal, Optional

from pydantic import BaseModel


class DeltaTableColumn(BaseModel):
    comment: Optional[str] = None
    """User-provided free-form text description."""

    name: str
    """Name of Column."""

    nullable: bool = True
    """Whether field may be Null."""

    partition_index: Optional[int] = None
    """Partition index for column."""

    position: Optional[int] = None
    """Ordinal position of column (starting at position 0)."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_json: str
    """Full data type specification, JSON-serialized."""

    type_name: Literal[
        "BOOLEAN",
        "BYTE",
        "SHORT",
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "STRING",
        "BINARY",
        "DECIMAL",
        "INTERVAL",
        "ARRAY",
        "STRUCT",
        "MAP",
        "CHAR",
        "NULL",
        "USER_DEFINED_TYPE",
        "TABLE_TYPE",
    ]
    """Name of type (INT, STRUCT, MAP, etc.)."""

    type_precision: Optional[int] = None
    """Digits of precision; required for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required for DecimalTypes."""

    type_text: str
    """Full data type specification as SQL/catalogString text."""


class DeltaTable(BaseModel):
    comment: Optional[str] = None
    """User-provided free-form text description."""

    name: str
    """Name of schema, relative to parent catalog."""

    properties: Optional[dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    columns: Iterable[DeltaTableColumn]
    """List of columns in the table."""


class DeltaTableInfo(BaseModel):
    catalog_name: Optional[str] = None
    """Name of parent catalog."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this schema was created, in epoch milliseconds."""

    full_name: Optional[str] = None
    """Full name of schema, in form of **catalog_name**.**schema_name**."""

    name: Optional[str] = None
    """Name of schema, relative to parent catalog."""

    properties: Optional[dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    schema_id: Optional[str] = None
    """Unique identifier for the schema."""

    updated_at: Optional[int] = None
    """Time at which this schema was last modified, in epoch milliseconds."""

    columns: Iterable[DeltaTableColumn]
    """List of columns in the table."""


class DeltaTableMerge(BaseModel):
    catalog_name: str
    schema_name: str
    table_name: str
    values: list[object]  # data to merge into the table
    predicate: str  # condition for matching rows
    updates: Optional[dict[str, str]] = None
    """mapping of target column to source column"""


class DeltaTableInsert(BaseModel):
    catalog_name: str
    schema_name: str
    table_name: str
    values: list[object]  # data to merge into the table


class DeltaTableDelete(BaseModel):
    catalog_name: str
    schema_name: str
    table_name: str
    values: list[object]  # data to merge into the table
    predicate: str  # condition for matching rows
