import pytest

from daydreaming_dagster.models.content_combination import generate_combo_id


class TestComboIDGeneration:
    def test_combo_id_deterministic(self):
        concepts = ["concept_001", "concept_002"]
        id1 = generate_combo_id(concepts, "brief", 2)
        id2 = generate_combo_id(concepts, "brief", 2)
        assert id1 == id2
        assert id1.startswith("combo_v1_")
        assert len(id1) == len("combo_v1_") + 12

    def test_combo_id_order_independent(self):
        id1 = generate_combo_id(["concept_001", "concept_002"], "brief", 2)
        id2 = generate_combo_id(["concept_002", "concept_001"], "brief", 2)
        assert id1 == id2

    def test_combo_id_different_params_different_ids(self):
        concepts = ["concept_001", "concept_002"]
        id1 = generate_combo_id(concepts, "brief", 2)
        id2 = generate_combo_id(concepts, "detailed", 2)
        id3 = generate_combo_id(concepts, "brief", 3)
        assert len({id1, id2, id3}) == 3


