from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from daydreaming_dagster.cohorts import generate_spec_bundle, SpecGenerationError

class WizardError(RuntimeError):
    """Raised when the spec wizard cannot complete the requested action."""


def _available_templates(cohorts_dir: Path) -> list[tuple[str, float]]:
    if not cohorts_dir.exists():
        return []
    templates: list[tuple[str, float]] = []
    for entry in cohorts_dir.iterdir():
        if not entry.is_dir():
            continue
        spec_dir = entry / "spec"
        if not spec_dir.is_dir():
            continue
        try:
            mtime = spec_dir.stat().st_mtime
        except OSError:
            mtime = 0.0
        templates.append((entry.name, mtime))
    templates.sort(key=lambda item: item[1], reverse=True)
    return templates


def copy_spec_from_template(
    data_root: Path | str,
    cohort_id: str,
    template_cohort: str,
    *,
    overwrite: bool = False,
) -> Path:
    base = Path(data_root)
    cohorts_dir = base / "cohorts"
    template_spec = cohorts_dir / template_cohort / "spec"
    if not template_spec.exists():
        try:
            generate_spec_bundle(base, template_cohort, overwrite=False)
        except SpecGenerationError as exc:  # pragma: no cover - propagation tested via unit
            raise WizardError(str(exc)) from exc
    if not template_spec.exists():
        raise WizardError(f"template cohort spec not found: {template_spec}")

    destination = cohorts_dir / cohort_id / "spec"
    if destination.exists():
        if not overwrite:
            raise WizardError(f"spec already exists for cohort {cohort_id}")
        shutil.rmtree(destination)

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(template_spec, destination)
    return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactive helper for creating a cohort spec from an existing template.",
    )
    parser.add_argument("--cohort-id", dest="cohort_id", help="New cohort identifier")
    parser.add_argument(
        "--template",
        dest="template",
        help="Existing cohort to use as the spec template",
    )
    parser.add_argument(
        "--data-root",
        dest="data_root",
        type=Path,
        default=Path("data"),
        help="Project data root (default: ./data)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing spec directory for the new cohort.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List cohorts with existing specs and exit.",
    )
    return parser


def _prompt(prompt: str) -> str:
    return input(prompt).strip()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    data_root = args.data_root
    cohorts_dir = data_root / "cohorts"

    if args.list:
        templates = _available_templates(cohorts_dir)
        if not templates:
            print("No cohort specs found under", cohorts_dir)
        else:
            print("Available cohort templates (most recent first):")
            for idx, (name, _) in enumerate(templates, start=1):
                print(f"  {idx}. {name}")
        return 0

    cohort_id = (args.cohort_id or _prompt("Enter new cohort id: ")).strip()
    if not cohort_id:
        parser.error("cohort id cannot be empty")

    template = args.template
    if not template:
        templates = _available_templates(cohorts_dir)
        if not templates:
            parser.error("no cohorts with specs available; generate one first")
        print("Available cohorts with specs (most recent first):")
        for idx, (name, _) in enumerate(templates, start=1):
            default_marker = " (default)" if idx == 1 else ""
            print(f"  {idx}. {name}{default_marker}")
        selection = _prompt("Select template (press Enter for default): ")
        if not selection.strip():
            template = templates[0][0]
        else:
            # allow numeric index or direct cohort id
            selection = selection.strip()
            if selection.isdigit():
                idx = int(selection)
                if not (1 <= idx <= len(templates)):
                    parser.error(f"template index {idx} out of range")
                template = templates[idx - 1][0]
            else:
                template = selection
    template = template.strip()
    if not template:
        parser.error("template cohort cannot be empty")

    try:
        destination = copy_spec_from_template(
            data_root,
            cohort_id,
            template,
            overwrite=args.overwrite,
        )
    except WizardError as exc:
        parser.error(str(exc))

    print(f"Spec copied to {destination}. You can now tweak config.yaml or items as needed.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
