#!/usr/bin/env python3
"""Check glossary use, target layering, and the public-name allowance."""

from __future__ import annotations

import ast
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


def read_inventory(path: Path) -> tuple[list[str], list[str]]:
    names: list[str] = []
    errors: list[str] = []
    relative = path.relative_to(ROOT).as_posix()
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        name = raw.partition("#")[0].strip()
        if not name:
            continue
        if not all(IDENTIFIER.fullmatch(part) for part in name.split("::")):
            errors.append(f"{relative}:{number}: invalid name {name!r}")
        elif name in names:
            errors.append(f"{relative}:{number}: duplicate public name {name!r}")
        else:
            names.append(name)
    return names, errors


def cpp_declared_names(text: str) -> set[str]:
    """Names the installed headers declare in namespace TangoBulk: types and free functions
    bare, public data members and member functions as Type::name. Special members, operators,
    enumerators and nested namespaces are not names. Relies on clang-format's brace style."""
    text = re.sub(r"//[^\n]*|/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"^\s*#.*$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"(?<=\w)\{[^{}\n]*\}", "", text)  # a member's brace initializer
    text = re.sub(r"=\s*\{[^{}\n]*\}", "= 0", text)  # a default argument's
    names: set[str] = set()
    scopes: list[tuple[str, str]] = []  # (kind, name); kind: namespace, type, enum, other
    public = [True]
    statement = ""
    for char in text:
        if char not in "{};":
            statement += char
            continue
        head = " ".join(statement.split())
        statement = ""
        inside = [name for kind, name in scopes if kind == "namespace"]
        in_api = inside == ["TangoBulk"]
        owner = scopes[-1] if scopes else ("", "")
        if char == "{":
            match = re.match(r"(namespace|enum class|struct|class)\s+(\w+)", head)
            kind = {"namespace": "namespace", "enum class": "enum"}.get(
                match.group(1), "type") if match else "other"
            name = match.group(2) if match else ""
            if in_api and kind in ("type", "enum") and owner[0] == "namespace":
                names.add(name)
            scopes.append((kind, name))
            public.append(kind != "type" or match.group(1) == "struct")
            continue
        if char == "}":
            if scopes:
                scopes.pop()
                public.pop()
            continue
        head = re.sub(r"^(public|private|protected):\s*", lambda m: public.__setitem__(
            -1, m.group(1) == "public") or "", head)
        if not head or not in_api or not public[-1] or head.startswith(("friend", "using namespace")):
            continue
        if owner[0] == "enum" or owner[0] == "other":
            continue
        using = re.match(r"using\s+(\w+)\s*=", head)
        if using and owner[0] == "namespace":
            names.add(using.group(1))
            continue
        if re.match(r"(struct|class)\s+\w+$", head):
            continue  # a forward declaration
        prefix = f"{owner[1]}::" if owner[0] == "type" else ""
        if re.search(r"\boperator\b", head):
            continue
        call = re.search(r"(~?\w+)\s*\(", head)
        if call:
            name = call.group(1)
            if name.startswith("~") or name == owner[1]:
                continue
            names.add(prefix + name)
            continue
        if owner[0] == "type":
            for declarator in head.split(","):
                declarator = declarator.split("=")[0].strip()
                field = re.search(r"(\w+)$", declarator)
                if field:
                    names.add(prefix + field.group(1))
    return names


PYTHON_PROTOCOL = {
    "__init__", "__enter__", "__exit__", "__aenter__", "__aexit__", "__iter__", "__next__",
    "__aiter__", "__anext__", "__repr__", "__del__",
}


def python_declared_names(package: Path) -> set[str]:
    """The names a user of the package can reach: __all__, the keyword arguments of its
    functions, and the public methods, properties and attributes of its classes."""
    names: set[str] = set()
    for path in sorted(package.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
            ):
                names.update(ast.literal_eval(node.value))
            elif isinstance(node, ast.FunctionDef) and node.col_offset == 0:
                if not node.name.startswith("_"):
                    names.update(arg.arg for arg in node.args.kwonlyargs)
            elif isinstance(node, ast.ClassDef):
                for member in node.body:
                    if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if not member.name.startswith("_") or (
                        member.name.endswith("__") and member.name not in PYTHON_PROTOCOL
                    ):
                        names.add(member.name)
                    for target in ast.walk(member):
                        if (
                            isinstance(target, ast.Attribute)
                            and isinstance(target.ctx, ast.Store)
                            and isinstance(target.value, ast.Name)
                            and target.value.id == "self"
                            and not target.attr.startswith("_")
                        ):
                            names.add(target.attr)
    return names


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
            if not alone and "(" in phrase:
                continue  # "(for this)" and the like limit it to a meaning names cannot show
            phrase = re.sub(r"\s*\([^)]*\)", "", phrase).strip()
            normalized = tuple(words(phrase))
            if normalized:
                avoid.append((normalized, alone))
    return terms, avoid


def public_header_text() -> str:
    paths = sorted((ROOT / "include/tango-bulk").rglob("*"))
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in paths
        if path.is_file() and path.suffix in CPP_SUFFIXES
    )


def check_names(label: str, inventory: str, declared: set[str], limit: int) -> list[str]:
    names, errors = read_inventory(ROOT / inventory)
    if len(names) > limit:
        errors.append(f"{label}: {len(names)} public names exceed the {limit}-name allowance")
    for name in sorted(declared - set(names)):
        errors.append(f"{label}: {name!r} is public but not in {inventory}")
    for name in sorted(set(names) - declared):
        errors.append(f"{label}: {name!r} is in {inventory} but not public")

    valid_terms, avoid_terms = glossary_terms()
    for name in sorted(set(names) | declared):
        identifier_words = tuple(words(name.split("::")[-1]))
        if identifier_words in valid_terms:
            continue
        for avoided, alone in avoid_terms:
            width = len(avoided)
            if (alone and identifier_words == avoided) or (
                not alone
                and any(
                    identifier_words[index : index + width] == avoided
                    for index in range(len(identifier_words) - width + 1)
                )
            ):
                errors.append(f"{label}: public name {name!r} uses an avoided glossary term")
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

    limits = budget["project"]
    cpp = cpp_declared_names(public_header_text())
    python = python_declared_names(ROOT / "python/tango_bulk")
    errors = check_names("C++", "tools/public_names.txt", cpp, limits["public_name_limit"])
    errors += check_names(
        "Python", "tools/python_names.txt", python, limits["python_name_limit"]
    )
    errors.extend(check_layering())
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(
        f"architecture checks passed: {len(cpp)} C++ names (limit {limits['public_name_limit']}), "
        f"{len(python)} Python names (limit {limits['python_name_limit']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
