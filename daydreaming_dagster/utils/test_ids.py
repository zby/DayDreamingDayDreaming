from daydreaming_dagster.utils.ids import (
    compute_logical_key_id,
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    compute_logical_key_id_evaluation,
    new_doc_id,
    reserve_doc_id,
)


class TestIDs:
    def test_logical_key_id_deterministic(self):
        a = compute_logical_key_id("draft", parts=("comboX", "tmplA", "model1"))
        b = compute_logical_key_id("draft", parts=("comboX", "tmplA", "model1"))
        assert a == b
        assert len(a) == 16

    def test_stage_specific_helpers(self):
        d = compute_logical_key_id_draft("comboX", "tmplA", "m1")
        e = compute_logical_key_id_essay("doc123", "tmplB", "m2")
        v = compute_logical_key_id_evaluation("doc456", "tmplC", "m3")
        assert len(d) == 16 and len(e) == 16 and len(v) == 16
        # Ensure stage contributes to id differences
        assert d != e and e != v and d != v

    def test_new_doc_id_uniqueness(self):
        logical = compute_logical_key_id("draft", parts=("c1", "t1", "m1"))
        x = new_doc_id(logical, "runA", 1)
        y = new_doc_id(logical, "runA", 2)
        z = new_doc_id(logical, "runB", 1)
        assert x != y and x != z and y != z
        assert len(x) == 16

    def test_reserve_doc_id_basic(self):
        a = reserve_doc_id("draft", "comboA__tmplX__model1")
        b = reserve_doc_id("draft", "comboA__tmplX__model1")
        c = reserve_doc_id("draft", "comboA__tmplX__model1", run_id="r1")
        assert a == b  # stable without run_id/salt
        assert len(a) == 16
        assert a != c  # run_id perturbs id
