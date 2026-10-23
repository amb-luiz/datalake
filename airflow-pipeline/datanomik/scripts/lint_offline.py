"""Minimal dependency-free syntax/whitespace lint; does not replace the Ruff gate."""

import ast
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    errors, count = [], 0
    for folder in ("src", "dags", "tests", "scripts"):
        for path in sorted((root / folder).rglob("*.py")):
            source = path.read_text()
            label = path.relative_to(root)
            try:
                ast.parse(source, filename=str(path))
            except SyntaxError as error:
                errors.append(f"{label}:{error.lineno}: syntax error")
            for line_number, line in enumerate(source.splitlines(), 1):
                if line.rstrip() != line:
                    errors.append(f"{label}:{line_number}: trailing whitespace")
                if "\t" in line:
                    errors.append(f"{label}:{line_number}: tab character")
            if not source.endswith("\n"):
                errors.append(f"{label}: missing final newline")
            count += 1
    print("\n".join(errors) if errors else f"Offline syntax/whitespace lint passed: {count} Python files.")
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())
