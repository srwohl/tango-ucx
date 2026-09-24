#!/usr/bin/env python3
"""Check glossary use, target layering, and the public-name allowance."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")
CPP_SUFFIXES = {".h", ".hpp", ".cc", ".cpp", ".cxx", ".cu", ".cuh", ".inl"}


def words(value: str) -> list[str]:
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return re.findall(r"[A-Za-z0-9]+", value.lower())


def read_api_inventory() -> tuple[list[str], list[str]]:
    names: list[str] = []
    errors: list[str] = []
    inventory = ROOT / "tools/public_names.txt"
    for number, raw in enumerate(inventory.read_text(encoding="utf-8").splitlines(), 1):
        name = raw.partition("#")[0].strip()
        if not name:
            continue
        if not IDENTIFIER.fullmatch(name):
            errors.append(f"tools/public_names.txt:{number}: invalid identifier {name!r}")
        elif name in names:
            errors.append(f"tools/public_names.txt:{number}: duplicate public name {name!r}")
        else:
            names.append(name)
    return names, errors


def glossary_terms() -> tuple[set[tuple[str, ...]], list[tuple[tuple[str, ...], bool]]]:
    context = (ROOT / "CONTEXT.md").read_text(encoding="utf-8")
    terms = {
        tuple(words(match.group(1)))
        for match in re.finditer(r"^\*\*(.+?)\*\*:\s*$", context, re.MULTILINE)
    }
    avoid: list[tuple[tuple[str, ...], bool]] = []
    for match in re.finditer(r"^\s*_Avoid_:\s*(.+)$", context, re.MULTILINE):
        for phrase in match.group(1).split(","):
            phrase = phrase.strip()
            if not phrase:
                continue
            alone = "(alone)" in phrase
            phrase = re.sub(r"\s*\([^)]*\)", "", phrase).strip()
            normalized = tuple(words(phrase))
            if normalized:
                avoid.append((normalized, alone))
    return terms, avoid


def public_header_text() -> str:
    paths = list((ROOT / "include/tango-bulk").rglob("*"))
    paths.append(ROOT / "src/transport/transport.h")
    chunks = [
        path.read_text(encoding="utf-8")
        for path in paths
        if path.is_file() and path.suffix in CPP_SUFFIXES
    ]
    return "\n".join(chunks)


def check_names(names: list[str], limit: int) -> list[str]:
    errors: list[str] = []
    if len(names) > limit:
        errors.append(f"public-name count {len(names)} exceeds the {limit}-name allowance")

    headers = public_header_text()
    valid_terms, avoid_terms = glossary_terms()
    for name in names:
        if not re.search(rf"\b{re.escape(name)}\b", headers):
            errors.append(f"public name {name!r} is missing from the public headers")
            continue

        identifier_words = tuple(words(name))
        if identifier_words in valid_terms:
            continue
        for avoided, alone in avoid_terms:
            if alone and identifier_words == avoided:
                errors.append(f"public name {name!r} uses an avoided glossary term")
                break
            width = len(avoided)
            if not alone and any(
                identifier_words[index : index + width] == avoided
                for index in range(len(identifier_words) - width + 1)
            ):
                errors.append(f"public name {name!r} uses an avoided glossary term")
                break
    return errors


def source_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return [
        path
        for path in directory.rglob("*")
        if path.is_file() and path.suffix in CPP_SUFFIXES
    ]


def check_layering() -> list[str]:
    errors: list[str] = []
    for directory, forbidden, boundary in (
        (ROOT / "src/transport", ("tango/",), "UCX transport"),
        (ROOT / "src/tango", ("ucp/", "ucs/", "cuda_runtime", "cuda.h"), "Tango adapter"),
    ):
        for path in source_files(directory):
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                match = re.match(r"\s*#\s*include\s*[<\"]([^>\"]+)", line)
                if match and any(match.group(1).startswith(prefix) for prefix in forbidden):
                    relative = path.relative_to(ROOT).as_posix()
                    errors.append(
                        f"{relative}:{number}: {boundary} includes forbidden header "
                        f"{match.group(1)!r}"
                    )
    return errors


def main() -> int:
    try:
        with (ROOT / "budget.toml").open("rb") as budget_file:
            budget = tomllib.load(budget_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        print(f"budget configuration error: {error}", file=sys.stderr)
        return 2

    names, errors = read_api_inventory()
    errors.extend(check_names(names, budget["project"]["public_name_limit"]))
    errors.extend(check_layering())
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(
        f"architecture checks passed: {len(names)} public names, "
        f"limit {budget['project']['public_name_limit']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
