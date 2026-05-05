#!/usr/bin/env python3

import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional


EXPECTED_ORDER = [
    "std_no_alias",
    "std_alias",
    "third_party_non_k8s_no_alias",
    "third_party_non_k8s_alias",
    "k8s_alias",
    "k8s_no_alias",
    "longhorn_external_no_alias",
    "longhorn_external_alias",
    "this_repo_no_alias",
    "this_repo_alias",
]


GROUP_DESCRIPTIONS = {
    "std_no_alias": "standard library packages without alias",
    "std_alias": "standard library packages with alias",
    "third_party_non_k8s_no_alias": "third-party non-k8s packages without alias",
    "third_party_non_k8s_alias": "third-party non-k8s packages with alias",
    "k8s_alias": "k8s-related packages with alias",
    "k8s_no_alias": "k8s-related packages without alias",
    "longhorn_external_no_alias": "Longhorn component packages except this repo without alias",
    "longhorn_external_alias": "Longhorn component packages except this repo with alias",
    "this_repo_no_alias": "packages from this repo without alias",
    "this_repo_alias": "packages from this repo with alias",
}


@dataclass
class ImportEntry:
    line_no: int
    raw: str
    path: str
    alias: Optional[str]
    group: str


@dataclass
class ImportGroup:
    entries: List[ImportEntry]


IMPORT_RE = re.compile(
    r'^\s*(?:(?P<alias>[._A-Za-z][._A-Za-z0-9]*)\s+)?(?P<quote>"[^"]+")'
)


def read_module_path() -> str:
    if not os.path.exists("go.mod"):
        raise RuntimeError("go.mod not found")

    with open("go.mod", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("module "):
                return line.split()[1]

    raise RuntimeError("module path not found in go.mod")


def is_stdlib(path: str) -> bool:
    """
    Best-effort stdlib detection.

    Examples:
      context        -> stdlib
      path/filepath  -> stdlib
      github.com/foo -> third-party
      k8s.io/api     -> third-party
    """
    first = path.split("/", 1)[0]
    return "." not in first


def is_k8s(path: str) -> bool:
    return (
        path.startswith("k8s.io/")
        or path.startswith("sigs.k8s.io/")
    )


def is_longhorn(path: str) -> bool:
    return path.startswith("github.com/longhorn/")


def classify_import(path: str, alias: Optional[str], module_path: str) -> str:
    has_alias = alias is not None

    if is_stdlib(path):
        return "std_alias" if has_alias else "std_no_alias"

    if path == module_path or path.startswith(module_path + "/"):
        return "this_repo_alias" if has_alias else "this_repo_no_alias"

    if is_longhorn(path):
        return "longhorn_external_alias" if has_alias else "longhorn_external_no_alias"

    if is_k8s(path):
        return "k8s_alias" if has_alias else "k8s_no_alias"

    return "third_party_non_k8s_alias" if has_alias else "third_party_non_k8s_no_alias"


def strip_inline_comment(line: str) -> str:
    # Good enough for import lines because the import path is quoted.
    if "//" in line:
        return line.split("//", 1)[0].rstrip()
    return line.rstrip()


def parse_import_line(
    line: str,
    line_no: int,
    module_path: str,
) -> Optional[ImportEntry]:
    line = strip_inline_comment(line).strip()

    if not line:
        return None

    match = IMPORT_RE.match(line)
    if not match:
        return None

    alias = match.group("alias")
    path = match.group("quote").strip('"')
    group = classify_import(path, alias, module_path)

    return ImportEntry(
        line_no=line_no,
        raw=line,
        path=path,
        alias=alias,
        group=group,
    )


def parse_import_blocks(filepath: str, module_path: str) -> List[List[ImportGroup]]:
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    blocks: List[List[ImportGroup]] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Multi-line import block:
        #
        # import (
        #     "fmt"
        # )
        if re.match(r"^\s*import\s*\(\s*$", line):
            i += 1
            current_entries: List[ImportEntry] = []
            groups: List[ImportGroup] = []

            while i < len(lines):
                current_line = lines[i]
                line_no = i + 1

                if re.match(r"^\s*\)\s*$", current_line):
                    if current_entries:
                        groups.append(ImportGroup(entries=current_entries))
                    break

                stripped = current_line.strip()

                # Blank line separates import groups.
                if stripped == "":
                    if current_entries:
                        groups.append(ImportGroup(entries=current_entries))
                        current_entries = []
                    i += 1
                    continue

                # Comments are allowed and ignored.
                if stripped.startswith("//"):
                    i += 1
                    continue

                entry = parse_import_line(current_line, line_no, module_path)
                if entry:
                    current_entries.append(entry)

                i += 1

            if groups:
                blocks.append(groups)

        # Single-line import:
        #
        # import "fmt"
        elif re.match(r"^\s*import\s+", line):
            line_no = i + 1
            import_part = re.sub(r"^\s*import\s+", "", line)
            entry = parse_import_line(import_part, line_no, module_path)
            if entry:
                blocks.append([ImportGroup(entries=[entry])])

        i += 1

    return blocks


def format_entry(entry: ImportEntry) -> str:
    if entry.alias:
        return f'{entry.alias} "{entry.path}"'
    return f'"{entry.path}"'


def describe_order(order: List[str]) -> str:
    return "\n".join(
        f"  {idx + 1}. {group}: {GROUP_DESCRIPTIONS[group]}"
        for idx, group in enumerate(order)
    )


def check_group_order(groups: List[ImportGroup], filepath: str) -> List[str]:
    errors: List[str] = []

    actual_order = []

    for group in groups:
        group_types = {entry.group for entry in group.entries}

        if len(group_types) != 1:
            lines = ", ".join(str(entry.line_no) for entry in group.entries)
            imports = "\n".join(
                f"    line {entry.line_no}: {format_entry(entry)} -> {entry.group}"
                for entry in group.entries
            )

            errors.append(
                f"{filepath}: import group contains mixed categories at lines {lines}.\n"
                f"{imports}\n"
                f"Please separate different categories with a blank line."
            )
            continue

        group_type = next(iter(group_types))
        actual_order.append(group_type)

    last_index = -1

    for group_type in actual_order:
        if group_type not in EXPECTED_ORDER:
            errors.append(
                f"{filepath}: invalid import category {group_type}.\n"
                f"Expected order:\n{describe_order(EXPECTED_ORDER)}"
            )
            continue

        index = EXPECTED_ORDER.index(group_type)

        if index < last_index:
            errors.append(
                f"{filepath}: import groups are in the wrong order.\n"
                f"Actual order:\n"
                + "\n".join(f"  - {group}" for group in actual_order)
                + "\n"
                f"Expected order:\n{describe_order(EXPECTED_ORDER)}"
            )
            break

        last_index = index

    return errors


def check_group_sorting(groups: List[ImportGroup], filepath: str) -> List[str]:
    errors: List[str] = []

    for group in groups:
        entries = group.entries

        actual = [format_entry(entry) for entry in entries]
        expected = sorted(actual, key=lambda item: item.lower())

        if actual != expected:
            first_line = entries[0].line_no

            errors.append(
                f"{filepath}: import group starting at line {first_line} is not sorted.\n"
                f"Actual:\n"
                + "\n".join(f"  {item}" for item in actual)
                + "\n"
                f"Expected:\n"
                + "\n".join(f"  {item}" for item in expected)
            )

    return errors


def check_duplicate_imports(groups: List[ImportGroup], filepath: str) -> List[str]:
    errors: List[str] = []
    seen = {}

    for group in groups:
        for entry in group.entries:
            key = (entry.alias, entry.path)

            if key in seen:
                errors.append(
                    f"{filepath}: duplicate import {format_entry(entry)} "
                    f"at lines {seen[key]} and {entry.line_no}"
                )
            else:
                seen[key] = entry.line_no

    return errors


def check_file(filepath: str, module_path: str) -> List[str]:
    errors: List[str] = []

    if filepath.startswith("vendor/") or "/vendor/" in filepath:
        return errors

    blocks = parse_import_blocks(filepath, module_path)

    for groups in blocks:
        errors.extend(check_group_order(groups, filepath))
        errors.extend(check_group_sorting(groups, filepath))
        errors.extend(check_duplicate_imports(groups, filepath))

    return errors


def read_files_from_args_or_stdin() -> List[str]:
    files = sys.argv[1:]

    if files:
        return files

    if not sys.stdin.isatty():
        return [line.strip() for line in sys.stdin if line.strip()]

    return []


def main() -> int:
    files = read_files_from_args_or_stdin()

    if not files:
        print("No files to check.")
        return 0

    module_path = read_module_path()
    all_errors: List[str] = []

    for filepath in files:
        if not filepath.endswith(".go"):
            continue

        if filepath.startswith("vendor/") or "/vendor/" in filepath:
            continue

        if not os.path.exists(filepath):
            continue

        all_errors.extend(check_file(filepath, module_path))

    if all_errors:
        print("Go import convention check failed.\n")
        print("Expected import group order:")
        print(describe_order(EXPECTED_ORDER))
        print()

        for error in all_errors:
            print(error)
            print()

        return 1

    print("Go import convention check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
