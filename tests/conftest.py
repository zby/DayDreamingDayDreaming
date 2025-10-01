"""Pytest fixtures for Dagster integration testing."""

import pytest
from unittest.mock import Mock
from dagster import DagsterInstance, build_asset_context, DagsterRun
from dagster import ConfigurableResource
import tempfile
from pathlib import Path
import types

import os

from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig, StageSettings


# Ensure DAGSTER_HOME points to a temp path for all tests
# Matches project docs: set DAGSTER_HOME to an absolute path per test run
@pytest.fixture(scope="session", autouse=True)
def _dagster_home_env(tmp_path_factory):
    tmp_home = tmp_path_factory.mktemp("dagster_home")
    os.environ["DAGSTER_HOME"] = str(tmp_home)
    return str(tmp_home)



class MockLLMClient:
    """Stateful mock LLM client for testing."""
    
    def __init__(self, responses=None, should_fail=False):
        self.responses = responses or {}
        self.should_fail = should_fail
        self.call_count = 0
        self.calls_made = []
    
    def generate(self, prompt, model, temperature=0.7):
        if self.should_fail:
            raise Exception("Mock LLM API failure")
        
        self.call_count += 1
        call_info = {
            'prompt': prompt[:100] + "..." if len(prompt) > 100 else prompt,
            'model': model,
            'temperature': temperature
        }
        self.calls_made.append(call_info)
        
        if model in self.responses:
            return self.responses[model]
        return f"Mock response {self.call_count} for {model}"


class MockLLMResource(ConfigurableResource):
    """Reusable mock LLM resource for testing - matches LLMClientResource interface."""
    
    responses: dict = {}
    should_fail: bool = False
    
    def model_post_init(self, __context):
        """Called after pydantic initialization - safe place to create client."""
        super().model_post_init(__context)
        # Create a persistent client that can be accessed for call tracking
        self._client = MockLLMClient(responses=self.responses, should_fail=self.should_fail)
    
    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None) -> str:
        """Generate method that matches LLMClientResource interface."""
        # Ensure client exists (in case of timing issues)
        if not hasattr(self, '_client'):
            self._client = MockLLMClient(responses=self.responses, should_fail=self.should_fail)
        return self._client.generate(prompt, model, temperature)

    def generate_with_info(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None):
        """Info-returning variant used by assets; wraps generate()."""
        text = self.generate(prompt, model, temperature, max_tokens)
        return text, {"finish_reason": "stop", "truncated": False}
    
    @property
    def call_count(self):
        """Proxy to client's call count."""
        if hasattr(self, '_client'):
            return self._client.call_count
        return 0
    
    @property 
    def calls_made(self):
        """Proxy to client's calls made."""
        if hasattr(self, '_client'):
            return self._client.calls_made
        return []


@pytest.fixture
def ephemeral_instance():
    """Provide a fresh Dagster instance for each test."""
    return DagsterInstance.ephemeral()


@pytest.fixture
def mock_creative_llm():
    """Mock LLM that gives creative responses."""
    return MockLLMResource(
        responses={
            "deepseek/deepseek-r1:free": "This represents a breakthrough in daydreaming methodology, combining innovative approaches with systematic analysis.",
            "google/gemma-3-27b-it:free": "A creative synthesis emerges from these concepts, suggesting novel pathways for discovery."
        }
    )


@pytest.fixture
def mock_evaluator_llm():
    """Mock LLM that gives evaluation scores."""
    return MockLLMResource(
        responses={
            "deepseek/deepseek-r1:free": "**SCORE**: 8.5\n\nThis response demonstrates exceptional creativity and insight into the daydreaming process.",
            "google/gemma-3-27b-it:free": "**SCORE**: 7.2\n\nGood analysis with some creative elements, but could explore deeper connections."
        }
    )


@pytest.fixture
def mock_failing_llm():
    """Mock LLM that simulates API failures."""
    return MockLLMResource(should_fail=True)


@pytest.fixture
def test_resources_with_mock_llm(mock_creative_llm):
    """Complete resource set with mock LLM for testing.

    Lazily import Definitions to avoid heavy imports during test collection.
    """
    from daydreaming_dagster.definitions import defs  # local import for laziness
    return {**defs.resources, "openrouter_client": mock_creative_llm}


@pytest.fixture
def temp_directory():
    """Provide a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def small_test_data():
    """Provide minimal test data for faster tests."""
    import pandas as pd
    
    return {
        "concepts_metadata": pd.DataFrame([
            {"concept_id": "test-concept-1", "name": "Test Concept One"},
            {"concept_id": "test-concept-2", "name": "Test Concept Two"}
        ]),
        "generation_models": pd.DataFrame([
            {"model_name": "test-model", "active": True}
        ]),
        "evaluation_models": pd.DataFrame([
            {"model_name": "test-eval-model", "active": True}
        ])
    }


@pytest.fixture
def definitions_with_paths(tmp_path):
    """Build Definitions bound to a temporary data root and validate path wiring."""

    def _build():
        from daydreaming_dagster.data_layer.paths import Paths
        from daydreaming_dagster.definitions import STAGES, build_definitions

        data_root = tmp_path / "data"
        data_root.mkdir(parents=True, exist_ok=True)
        paths = Paths.from_str(data_root)
        defs = build_definitions(paths=paths)

        assert defs.resources["data_root"] == str(paths.data_root)

        csv_expectations = {
            "csv_io_manager": paths.tasks_dir,
            "parsing_results_io_manager": paths.parsing_dir,
            "summary_results_io_manager": paths.summary_dir,
            "cross_experiment_io_manager": paths.cross_experiment_dir,
            "error_log_io_manager": paths.data_root / "7_reporting",
        }
        for key, expected in csv_expectations.items():
            manager = defs.resources[key]
            assert getattr(manager, "base_path") == expected

        in_memory_manager = defs.resources["in_memory_io_manager"]
        assert getattr(in_memory_manager, "_fallback_root") == paths.data_root

        for entry in STAGES.values():
            for resource_key in entry.resource_factories:
                resource = defs.resources[resource_key]
                if resource_key.endswith("prompt_io_manager"):
                    assert getattr(resource, "gens_root") == paths.gens_root
                else:
                    assert getattr(resource, "_fallback_root", None) == paths.data_root

        return defs, paths

    return _build


# ------------------------------

@pytest.fixture
def make_ctx():
    """Factory for a minimal Dagster-like context object.

    Usage: ctx = make_ctx(partition_key, data_root, llm=..., min_draft_lines=3, membership_service=...)
    Provides:
      - context.partition_key
      - context.run.run_id (for metadata)
      - context.log.info (no-op)
      - context.resources with data_root, experiment_config, openrouter_client, optional membership_service
      - context.add_output_metadata(md) (no-op by default)
    """

    def _make(
        partition_key: str,
        data_root: Path,
        *,
        llm=None,
        exp_config=None,
        min_draft_lines: int | None = None,
        membership_service=None,
        run_id: str = "RUNTEST",
        asset: bool = True,
    ):
        def _resolve_experiment_config():
            if exp_config is not None:
                return exp_config
            cfg = ExperimentConfig()
            if min_draft_lines is None:
                return cfg
            stage_config = dict(cfg.stage_config)
            draft_settings = stage_config.get("draft", StageSettings())
            stage_config["draft"] = StageSettings(
                generation_max_tokens=draft_settings.generation_max_tokens,
                min_lines=int(min_draft_lines),
            )
            return ExperimentConfig(stage_config=stage_config)

        class _Log:
            def info(self, *_args, **_kwargs):
                return None

        class _Ctx:
            def __init__(self):
                self.partition_key = partition_key
                self.log = _Log()
                self.run = types.SimpleNamespace(run_id=run_id)
                self.resources = types.SimpleNamespace()
                self.resources.data_root = str(data_root)
                self.resources.experiment_config = _resolve_experiment_config()
                if llm is not None:
                    self.resources.openrouter_client = llm
                if membership_service is not None:
                    self.resources.membership_service = membership_service

            def add_output_metadata(self, _md: dict):
                return None

        if not asset:
            return _Ctx()

        resources = {
            "data_root": str(data_root),
            "experiment_config": _resolve_experiment_config(),
        }

        if llm is not None:
            resources["openrouter_client"] = llm
        if membership_service is not None:
            resources["membership_service"] = membership_service

        ctx = build_asset_context(
            partition_key=partition_key,
            resources=resources,
            run_tags={"run_id": run_id, "dagster/run/id": run_id},
        )
        try:
            setattr(ctx, "run", types.SimpleNamespace(run_id=run_id))
        except Exception:
            # Best-effort; some contexts forbid attribute assignment.
            pass
        try:
            ctx._op_execution_context._dagster_run = DagsterRun(
                job_name="test_job",
                run_id=run_id,
            )
        except Exception:
            pass
        return ctx

    return _make
# Helper-style integration support
# ------------------------------

import os
import copy


def _mk_response(prompt: str, *, lines: int, tail_score: str) -> str:
    """Deterministic-ish LLM body used by helper-based tests.

    The last line embeds a SCORE token for evaluation parsing.
    """
    # Normalize to avoid negative hashes differing across runs
    seed = abs(hash(prompt)) % 1000
    chunks = [f"L{i+1}: {seed}" for i in range(max(int(lines), 1))]
    body = "\n".join(chunks)
    return f"{body}\nSCORE: {tail_score}"


@pytest.fixture
def mock_llm(request):
    """Configurable mock LLM implementing generate_with_info(prompt, model, max_tokens).

    Configure via @pytest.mark.llm_cfg(lines=..., truncated=..., tail_score=...).
    Defaults: lines=3, truncated=False, tail_score="7.0".
    """
    cfg = request.node.get_closest_marker("llm_cfg")
    lines = (cfg.kwargs.get("lines") if cfg else None) or 3
    truncated = (cfg.kwargs.get("truncated") if cfg else None) or False
    tail_score = (cfg.kwargs.get("tail_score") if cfg else None) or "7.0"

    class _FakeLLM:
        def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
            text = _mk_response(prompt, lines=lines, tail_score=tail_score)
            info = {
                "finish_reason": "stop",
                "truncated": bool(truncated),
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }
            return text, info

    return _FakeLLM()


@pytest.fixture
def canon_meta():
    """Canonicalize metadata dicts by dropping volatile fields and baselining file paths."""
    def _canon(meta: dict) -> dict:
        m = copy.deepcopy(meta)
        m.pop("duration_s", None)
        files = m.get("files")
        if isinstance(files, dict):
            m["files"] = {k: (os.path.basename(v) if isinstance(v, str) else v) for k, v in files.items()}
        return m

    return _canon


@pytest.fixture
def tiny_data_root(tmp_path: Path) -> Path:
    """Lay down a minimal data root for helper-based integration tests.

    Structure mirrors project expectations under <tmp>/1_raw and templates.
    """
    base = tmp_path
    raw = base / "1_raw"
    (raw / "templates" / "draft").mkdir(parents=True, exist_ok=True)
    (raw / "templates" / "essay").mkdir(parents=True, exist_ok=True)
    (raw / "templates" / "evaluation").mkdir(parents=True, exist_ok=True)
    # CSVs
    (raw / "llm_models.csv").write_text(
        "model_id,for_generation,for_evaluation,active\n"
        "m-gen,True,False,True\n"
        "m-eval,False,True,True\n",
        encoding="utf-8",
    )
    (raw / "draft_templates.csv").write_text(
        "template_id,template_name,description,parser,generator,active\n"
        "test-draft,T Draft,Desc,essay_block,llm,True\n",
        encoding="utf-8",
    )
    (raw / "essay_templates.csv").write_text(
        "template_id,template_name,description,parser,generator,active\n"
        "test-essay-llm,T Essay,Desc,identity,llm,True\n"
        "test-essay-copy,T Essay,Desc,identity,copy,True\n",
        encoding="utf-8",
    )
    (raw / "evaluation_templates.csv").write_text(
        "template_id,template_name,description,parser,active\n"
        "test-eval,T Eval,Desc,in_last_line,True\n",
        encoding="utf-8",
    )
    # Templates
    (raw / "templates" / "draft" / "test-draft.txt").write_text(
        "Draft for: {% for c in concepts %}{{ c.name }};{% endfor %}", encoding="utf-8"
    )
    (raw / "templates" / "essay" / "test-essay-llm.txt").write_text(
        "Essay:\n{{ draft_block }}\n--\n{{ links_block }}",
        encoding="utf-8",
    )
    (raw / "templates" / "essay" / "test-essay-copy.txt").write_text("Copy essay", encoding="utf-8")
    (raw / "templates" / "evaluation" / "test-eval.txt").write_text("Score this: {{ response }}", encoding="utf-8")
    # gens root
    (base / "gens").mkdir(parents=True, exist_ok=True)
    return base
