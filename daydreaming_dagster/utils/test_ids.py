from daydreaming_dagster.utils.ids import reserve_gen_id


class TestIDs:
    def test_reserve_gen_id_basic(self):
        a = reserve_gen_id("draft", "comboA__tmplX__model1")
        b = reserve_gen_id("draft", "comboA__tmplX__model1")
        c = reserve_gen_id("draft", "comboA__tmplX__model1", run_id="r1")
        d = reserve_gen_id("draft", "comboA__tmplX__model1", salt="s1")
        # Stable without run_id/salt
        assert a == b and len(a) == 16
        # run_id or salt perturbs id
        assert a != c and a != d
