from __future__ import annotations

from typing import Any, Dict, Optional


def build_generation_metadata(
    *,
    stage: str,
    gen_id: str,
    parent_gen_id: Optional[str],
    template_id: Optional[str],
    model_id: Optional[str],
    task_id: Optional[str],
    function: str,
    run_id: Optional[str] = None,
    usage: Optional[dict] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a normalized metadata.json dictionary for gens-store outputs.

    Standard keys across stages:
    - stage, gen_id, parent_gen_id
    - template_id, model_id, task_id
    - function (asset function name)
    - run_id (Dagster run id when available)
    - usage (token/latency info if present)

    Extra keys can be merged via `extra`.
    """
    md: Dict[str, Any] = {
        "stage": stage,
        "gen_id": str(gen_id) if gen_id is not None else "",
        "parent_gen_id": str(parent_gen_id) if parent_gen_id else "",
        "template_id": str(template_id) if template_id else "",
        "model_id": str(model_id) if model_id else "",
        "task_id": str(task_id) if task_id else "",
        "function": function,
    }
    if run_id:
        md["run_id"] = str(run_id)
    if usage is not None:
        md["usage"] = usage
    if extra:
        md.update(extra)
    return md

