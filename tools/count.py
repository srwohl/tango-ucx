#!/usr/bin/env python3
"""Count product and test source against the selected final-shape allowances."""

from __future__ import annotations

import argparse
import fnmatch
import sys
import tomllib
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_SUFFIXES = {
    ".h",
    ".hpp",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".py",
    ".cu",
    ".cuh",
    ".inl",
}


def load_budget() -> dict:
    with (ROOT / "budget.toml").open("rb") as budget_file:
        return tomllib.load(budget_file)


def source_files() -> list[Path]:
    paths: list[Path] = []
    for root_name in ("include", "src", "python", "tests"):
        root = ROOT / root_name
        if root.exists():
            paths.extend(
                path
                for path in root.rglob("*")
                if path.is_file() and path.suffix in SOURCE_SUFFIXES
            )
    return sorted(paths)


def category(path: Path) -> str:
    relative = path.relative_to(ROOT).as_posix()
    if relative.startswith("tests/") or relative.startswith("python/tests/"):
        return "tests"
    return "library"


def line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def active_allowances(entries: list[dict], phase: int) -> dict[str, dict]:
    groups: dict[str, dict] = {}
    for entry in entries:
        if entry["phase"] > phase:
            continue
        group = groups.setdefault(
            entry["name"],
            {"kind": entry["kind"], "patterns": set(), "limit": 0},
        )
        if group["kind"] != entry["kind"]:
            raise ValueError(f"allowance {entry['name']!r} changes category")
        patterns = set(entry["patterns"])
        group["patterns"].update(patterns)
        group["limit"] += entry["lines"]
    return groups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        type=int,
        choices=(1, 2, 3, 4, 5),
        help="phase to count (defaults to project.active_phase in budget.toml)",
    )
    args = parser.parse_args()

    try:
        budget = load_budget()
        phase = args.phase or budget["project"]["active_phase"]
        phase_budget = budget["phases"][str(phase)]
        groups = active_allowances(budget["allowance"], phase)
    except (KeyError, OSError, tomllib.TOMLDecodeError, ValueError) as error:
        print(f"budget configuration error: {error}", file=sys.stderr)
        return 2

    files = source_files()
    totals = defaultdict(int)
    group_counts = defaultdict(int)
    problems: list[str] = []

    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        kind = category(path)
        lines = line_count(path)
        totals[kind] += lines

        matching = [
            name
            for name, group in groups.items()
            if any(fnmatch.fnmatchcase(relative, pattern) for pattern in group["patterns"])
        ]
        if len(matching) != 1:
            detail = "no allowance" if not matching else f"multiple allowances: {matching}"
            problems.append(f"{relative}: {detail}")
            continue

        name = matching[0]
        if groups[name]["kind"] != kind:
            problems.append(f"{relative}: allowance {name!r} has the wrong category")
            continue
        group_counts[name] += lines

    for name, count in sorted(group_counts.items()):
        allowance = groups[name]["limit"]
        print(f"{name:20} {count:5} / {allowance:5} lines")
        if count > allowance:
            problems.append(f"{name}: {count} lines exceeds its {allowance}-line allowance")

    total = totals["library"] + totals["tests"]
    ceiling = budget["project"]["line_ceiling"]
    print(
        f"phase {phase}: library {totals['library']} / {phase_budget['library']}, "
        f"tests {totals['tests']} / {phase_budget['tests']}, "
        f"combined {total} / {ceiling} lines"
    )

    if totals["library"] > phase_budget["library"]:
        problems.append(
            f"library total {totals['library']} exceeds phase {phase} allowance "
            f"{phase_budget['library']}"
        )
    if totals["tests"] > phase_budget["tests"]:
        problems.append(
            f"test total {totals['tests']} exceeds phase {phase} allowance "
            f"{phase_budget['tests']}"
        )
    if total > ceiling:
        problems.append(f"combined total {total} exceeds the {ceiling}-line ceiling")

    if problems:
        for problem in problems:
            print(f"ERROR: {problem}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
