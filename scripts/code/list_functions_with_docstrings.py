#!/usr/bin/env python3
"""
List functions (and optional methods) in Python packages under a directory,
along with their docstrings, without importing the code (AST-based, no side effects).

Usage examples:

  # Scan the current repo's packages and print Markdown summary
  python scripts/code/list_functions_with_docstrings.py --root . --format md

  # JSON output, include nested functions, and exclude legacy/scripts
  python scripts/code/list_functions_with_docstrings.py \
      --root . --format json --include-nested \
      --exclude scripts --exclude data --exclude .venv

  # Exclude functions by prefix (e.g., pytest's test_). This is also implied
  # when you pass `--exclude tests`.
  python scripts/code/list_functions_with_docstrings.py --root daydreaming_dagster \
      --exclude-func-prefix test_ --format md
Notes:
  - Only files inside Python packages (directories with __init__.py) are included.
  - By default, class methods are included; nested functions are excluded.
  - Uses only the Python standard library (ast, argparse, json, csv, pathlib).
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple


DEFAULT_EXCLUDES = {
    ".git",
    ".hg",
    ".svn",
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "build",
    "dist",
    "data",
}


@dataclass
class FuncRecord:
    module: str
    qualname: str
    kind: str  # function | method
    is_async: bool
    nested: bool
    file: str
    lineno: int
    decorators: List[str]
    docstring: Optional[str]


def is_excluded(path: Path, root: Path, exclude_names: Set[str]) -> bool:
    try:
        rel_parts = path.relative_to(root).parts
    except Exception:
        rel_parts = path.parts
    return any(part in exclude_names for part in rel_parts)


def find_package_root(file_path: Path, stop_dir: Path) -> Optional[Path]:
    """Return the nearest package ancestor up to stop_dir (inclusive).

    Walk up from file_path.parent to stop_dir and remember the highest directory
    that contains an __init__.py. Returns None if no ancestor is a package.
    Works with namespace-like subfolders (no __init__.py) under a package.
    """
    cur = file_path.parent
    last_pkg: Optional[Path] = None
    while True:
        if (cur / "__init__.py").exists():
            last_pkg = cur
        if cur == stop_dir or cur.parent == cur:
            break
        cur = cur.parent
    return last_pkg


def dotted_module_name(file_path: Path, pkg_root: Path) -> str:
    """Compute dotted module path from the package root.

    For __init__.py, returns the package path (without .__init__).
    """
    rel = file_path.relative_to(pkg_root)
    if file_path.name == "__init__.py":
        parts = [*pkg_root.parts[-1:]] + list(rel.parent.parts)
    else:
        parts = [*pkg_root.parts[-1:]] + list(rel.with_suffix("").parts)
    return ".".join(parts)


def decorator_names(node: ast.AST) -> List[str]:
    out: List[str] = []
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return out
    for dec in node.decorator_list:
        name = None
        if isinstance(dec, ast.Name):
            name = dec.id
        elif isinstance(dec, ast.Attribute):
            # attribute chain a.b.c
            parts: List[str] = []
            cur: Optional[ast.AST] = dec
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                parts.append(cur.id)
            parts.reverse()
            name = ".".join(parts)
        elif isinstance(dec, ast.Call):
            # decorator with arguments, e.g., @dec(...)
            func = dec.func
            if isinstance(func, ast.Name):
                name = func.id + "(...)"
            elif isinstance(func, ast.Attribute):
                # like mod.dec(...)
                parts: List[str] = []
                cur: Optional[ast.AST] = func
                while isinstance(cur, ast.Attribute):
                    parts.append(cur.attr)
                    cur = cur.value
                if isinstance(cur, ast.Name):
                    parts.append(cur.id)
                parts.reverse()
                name = ".".join(parts) + "(...)"
        if name:
            out.append(name)
    return out


class FunctionCollector(ast.NodeVisitor):
    def __init__(
        self,
        module: str,
        file_path: Path,
        include_methods: bool,
        include_nested: bool,
        rel_file: str,
    ) -> None:
        self.module = module
        self.file_path = file_path
        self.include_methods = include_methods
        self.include_nested = include_nested
        self.rel_file = rel_file
        self.class_stack: List[str] = []
        self.func_stack: List[str] = []
        self.records: List[FuncRecord] = []

    def _record(self, node: ast.AST, name: str, is_async: bool) -> None:
        nested = len(self.func_stack) > 0
        in_class = len(self.class_stack) > 0
        if nested and not self.include_nested:
            return
        if in_class and not self.include_methods:
            return
        # Build qualname: ClassA.ClassB.outer.inner.func (if nested included)
        parts: List[str] = []
        if self.class_stack:
            parts.extend(self.class_stack)
        if self.func_stack and self.include_nested:
            parts.extend(self.func_stack)
        parts.append(name)
        qualname = ".".join(parts)
        kind = "method" if in_class else "function"
        doc = ast.get_docstring(node, clean=True)
        lineno = getattr(node, "lineno", 0)
        rec = FuncRecord(
            module=self.module,
            qualname=qualname,
            kind=kind,
            is_async=is_async,
            nested=nested,
            file=self.rel_file,
            lineno=lineno,
            decorators=decorator_names(node),
            docstring=doc,
        )
        self.records.append(rec)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def _visit_func(self, node: ast.AST, name: str, is_async: bool) -> None:
        self._record(node, name, is_async)
        self.func_stack.append(name)
        self.generic_visit(node)
        self.func_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # type: ignore[override]
        self._visit_func(node, node.name, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # type: ignore[override]
        self._visit_func(node, node.name, is_async=True)


def iter_python_files(root: Path, exclude: Set[str]) -> Iterable[Path]:
    for p in root.rglob("*.py"):
        if is_excluded(p, root, exclude):
            continue
        yield p


def collect_from_file(
    file_path: Path,
    root: Path,
    include_methods: bool,
    include_nested: bool,
    relative_paths: bool,
) -> List[FuncRecord]:
    pkg_root = find_package_root(file_path, root)
    if not pkg_root:
        return []
    module = dotted_module_name(file_path, pkg_root)
    rel_file = str(file_path.relative_to(root)) if relative_paths else str(file_path)
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except Exception as e:  # pragma: no cover - defensive I/O
        print(f"[warn] Failed to parse {file_path}: {e}", file=sys.stderr)
        return []
    visitor = FunctionCollector(
        module=module,
        file_path=file_path,
        include_methods=include_methods,
        include_nested=include_nested,
        rel_file=rel_file,
    )
    visitor.visit(tree)
    return visitor.records


def to_markdown(records: List[FuncRecord], first_line_only: bool) -> str:
    lines: List[str] = []
    lines.append("# Functions and docstrings\n")
    # Group by module for readability
    by_mod: dict[str, List[FuncRecord]] = {}
    for r in records:
        by_mod.setdefault(r.module, []).append(r)
    for mod in sorted(by_mod):
        lines.append(f"## {mod}\n")
        for r in sorted(by_mod[mod], key=lambda x: (x.file, x.lineno, x.qualname)):
            header = (
                f"- {r.kind}: {r.qualname}"
                + (" [async]" if r.is_async else "")
                + (" [nested]" if r.nested else "")
                + f"  ({r.file}:{r.lineno})"
            )
            lines.append(header)
            if r.docstring:
                doc = r.docstring.strip()
                if first_line_only:
                    doc = doc.splitlines()[0] if doc else ""
                lines.append(f"  - doc: {doc}\n")
            else:
                lines.append("  - doc: (none)\n")
    return "\n".join(lines)


def to_csv(records: List[FuncRecord]) -> str:
    import io

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "module",
            "qualname",
            "kind",
            "is_async",
            "nested",
            "file",
            "lineno",
            "decorators",
            "docstring",
        ]
    )
    for r in records:
        writer.writerow(
            [
                r.module,
                r.qualname,
                r.kind,
                int(r.is_async),
                int(r.nested),
                r.file,
                r.lineno,
                ";".join(r.decorators),
                (r.docstring or "").replace("\r", " ").replace("\n", "\\n"),
            ]
        )
    return buf.getvalue()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="List functions in Python packages under a directory with docstrings"
    )
    ap.add_argument("--root", type=Path, default=Path("."), help="Directory to scan")
    ap.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Directory names to exclude (can be repeated)",
    )
    ap.add_argument(
        "--format",
        choices=["json", "md", "csv"],
        default="md",
        help="Output format",
    )
    ap.add_argument(
        "--exclude-func-prefix",
        action="append",
        default=[],
        help=(
            "Exclude functions whose final name starts with any given prefix. "
            "Repeatable. Example: --exclude-func-prefix test_"
        ),
    )
    ap.add_argument(
        "--no-methods",
        action="store_true",
        help="Exclude class/instance methods; only module-level functions",
    )
    ap.add_argument(
        "--include-nested",
        action="store_true",
        help="Include nested functions (functions defined inside functions)",
    )
    ap.add_argument(
        "--absolute-paths",
        action="store_true",
        help="Print absolute file paths instead of paths relative to root",
    )
    ap.add_argument(
        "--full-doc",
        action="store_true",
        help="For markdown, include full docstrings (default: first line only)",
    )
    args = ap.parse_args(argv)

    root = args.root.resolve()
    exclude = set(DEFAULT_EXCLUDES)
    if args.exclude:
        exclude.update(args.exclude)

    all_records: List[FuncRecord] = []
    for py in iter_python_files(root, exclude):
        recs = collect_from_file(
            py,
            root=root,
            include_methods=not args.no_methods,
            include_nested=args.include_nested,
            relative_paths=not args.absolute_paths,
        )
        if recs:
            all_records.extend(recs)

    # Implicitly exclude pytest-style test functions when user excludes 'tests' dirs
    # Matches [tool.pytest.ini_options] python_functions = ["test_*"] default here.
    implied_prefixes: List[str] = []
    if any(x == "tests" for x in (args.exclude or [])):
        implied_prefixes.append("test_")

    # Optional filter: exclude functions by raw-name prefix (e.g., test_)
    all_prefixes = list(args.exclude_func_prefix or []) + implied_prefixes
    if all_prefixes:
        prefixes = tuple(all_prefixes)
        def _keep(rec: FuncRecord) -> bool:
            raw = rec.qualname.split(".")[-1]
            return not raw.startswith(prefixes)
        all_records = [r for r in all_records if _keep(r)]

    # Sort for stable output
    all_records.sort(key=lambda r: (r.module, r.file, r.lineno, r.qualname))

    if args.format == "json":
        print(json.dumps([asdict(r) for r in all_records], indent=2, ensure_ascii=False))
    elif args.format == "csv":
        print(to_csv(all_records), end="")
    else:  # md
        print(to_markdown(all_records, first_line_only=not args.full_doc))

    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
