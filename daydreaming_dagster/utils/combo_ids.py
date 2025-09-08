from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd


class ComboIDManager:
    """Manage the global append-only mapping of combo IDs to their components.

    The mapping is normalized: one row per concept per combo.
    """

    def __init__(self, mappings_path: str = "data/combo_mappings.csv"):
        self.mappings_path = Path(mappings_path)

    def get_or_create_combo_id(
        self, concept_ids: List[str], description_level: str, k_max: int
    ) -> str:
        """Return a stable combo ID, writing to the mapping if it's new.

        Collision safety: If an existing combo_id is found with different
        parameters or concept set, raise a ValueError so callers can react.
        """
        # Generate ID using the model's canonical function (kept close to the data model)
        from daydreaming_dagster.models.content_combination import (
            generate_combo_id as _gen_combo_id,
        )

        combo_id = _gen_combo_id(concept_ids, description_level, k_max)

        if self.mappings_path.exists():
            existing_mappings = pd.read_csv(self.mappings_path)
            existing_combo = existing_mappings[existing_mappings["combo_id"] == combo_id]
            if not existing_combo.empty:
                # Validate that existing rows match current parameters
                # Compare description_level and k_max (consistent across rows)
                sample_row = existing_combo.iloc[0]
                same_params = (
                    str(sample_row["description_level"]) == str(description_level)
                    and int(sample_row["k_max"]) == int(k_max)
                )

                # Compare concept sets
                existing_concepts = set(existing_combo["concept_id"].astype(str).tolist())
                requested_concepts = set(map(str, concept_ids))
                same_concepts = existing_concepts == requested_concepts

                if same_params and same_concepts:
                    return combo_id

                raise ValueError(
                    f"combo_id collision detected for {combo_id}: existing params/concepts do not match."
                )

        # Append new mapping rows (one per concept)
        new_rows = []
        for concept_id in sorted(concept_ids):
            new_rows.append(
                {
                    "combo_id": combo_id,
                    "version": "v1",
                    "concept_id": concept_id,
                    "description_level": description_level,
                    "k_max": int(k_max),
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

        new_df = pd.DataFrame(new_rows)

        if self.mappings_path.exists():
            existing_df = pd.read_csv(self.mappings_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
            self.mappings_path.parent.mkdir(parents=True, exist_ok=True)

        combined_df.to_csv(self.mappings_path, index=False)
        return combo_id

    def load_mappings(self) -> pd.DataFrame:
        if self.mappings_path.exists():
            return pd.read_csv(self.mappings_path)
        return pd.DataFrame(
            columns=[
                "combo_id",
                "version",
                "concept_id",
                "description_level",
                "k_max",
                "created_at",
            ]
        )
