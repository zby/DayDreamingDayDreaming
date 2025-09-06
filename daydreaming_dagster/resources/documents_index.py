from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dagster import ConfigurableResource
from ..utils.documents_index import SQLiteDocumentsIndex


@dataclass
class DocumentsConfig:
    db_path: Path
    docs_root: Path


class DocumentsIndexResource(ConfigurableResource):
    """
    Dagster resource wrapping the standalone SQLiteDocumentsIndex.

    Exposes feature flags via environment variables (no behavior change unless used by assets):
    - DD_DOCS_INDEX_ENABLED (default: false)
    - DD_DOCS_LEGACY_WRITE_ENABLED (default: true)
    - DD_DOCS_PROMPT_COPY_ENABLED (default: true)
    """

    db_path: str = str(Path("data") / "db" / "documents.sqlite")
    docs_root: str = str(Path("data") / "docs")

    def get_index(self) -> SQLiteDocumentsIndex:
        idx = SQLiteDocumentsIndex(Path(self.db_path), Path(self.docs_root))
        idx.init_maybe_create_tables()
        return idx

    # Feature flags: read envs for now; assets may consult these flags explicitly
    @property
    def index_enabled(self) -> bool:
        return os.getenv("DD_DOCS_INDEX_ENABLED", "0") in ("1", "true", "True")

    @property
    def legacy_write_enabled(self) -> bool:
        return os.getenv("DD_DOCS_LEGACY_WRITE_ENABLED", "1") in ("1", "true", "True")

    @property
    def prompt_copy_enabled(self) -> bool:
        return os.getenv("DD_DOCS_PROMPT_COPY_ENABLED", "1") in ("1", "true", "True")

