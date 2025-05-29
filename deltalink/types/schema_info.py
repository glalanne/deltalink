from typing import Optional

from pydantic import BaseModel


class SchemaInfo(BaseModel):
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
