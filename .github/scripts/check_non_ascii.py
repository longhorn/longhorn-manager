#!/usr/bin/env python3
"""Reject non-ASCII characters added by a pull request (Trojan Source protection).

Usage: check_non_ascii.py <base-ref>

Only lines added in `git diff <base-ref>...HEAD` are checked. Paths matching a
pattern in .github/non-ascii-allowlist.txt may contain harmless non-ASCII
characters, but BiDi controls and zero-width characters are always rejected.
"""
import fnmatch
import re
import subprocess
import sys
import unicodedata

ALLOWLIST_FILE = ".github/non-ascii-allowlist.txt"
EXCLUDED = [
    "vendor/*", "*/vendor/*", "go.sum", "*.pb.go", "zz_generated*", "*/zz_generated*",
]
DANGEROUS = re.compile(
    "[\u202a-\u202e\u2066-\u2069\u200b-\u200f\u2060\ufeff\u061c\u180e]"
)
SUGGESTIONS = {
    "\u2014": "--", "\u2013": "-", "\u2192": "->", "\u2190": "<-",
    "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
    "\u2026": "...", "\u00d7": "x", "\u2500": "-", "\u00a0": " ",
}
HUNK = re.compile(r"^@@ -\S+ \+(\d+)(?:,\d+)? @@")


def load_allowlist():
    try:
        with open(ALLOWLIST_FILE, encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        return []


def matches(path, patterns):
    return any(fnmatch.fnmatch(path, p) for p in patterns)


def main():
    base = sys.argv[1]
    allow = load_allowlist()
    diff = subprocess.run(
        ["git", "-c", "core.quotepath=off", "diff", "-U0", "--no-color",
         "--diff-filter=ACMRT", f"{base}...HEAD"],
        check=True, capture_output=True, text=True, encoding="utf-8", errors="replace",
    ).stdout

    errors = 0
    path, lineno = None, 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
            continue
        m = HUNK.match(line)
        if m:
            lineno = int(m.group(1))
            continue
        if not line.startswith("+") or path is None:
            continue
        text = line[1:]
        cur = lineno
        lineno += 1
        if matches(path, EXCLUDED):
            continue
        allowed = matches(path, allow)
        for col, ch in enumerate(text, 1):
            if ord(ch) < 0x80:
                continue
            if allowed and not DANGEROUS.match(ch):
                continue
            name = unicodedata.name(ch, "UNKNOWN")
            hint = SUGGESTIONS.get(ch)
            suffix = f" (suggest: {hint!r})" if hint else ""
            print(f"{path}:{cur}:{col}: U+{ord(ch):04X} {name}{suffix}")
            errors += 1

    if errors:
        print(f"\n{errors} non-ASCII character(s) found in added lines. "
              f"Use ASCII or escape sequences; intentional cases go in {ALLOWLIST_FILE}.")
        return 1
    print("No non-ASCII characters added.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
