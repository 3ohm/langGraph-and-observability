"""Rename the package and the distribution of a fork (spec, header and §13.1).

Run it from the repository root::

    uv run scripts/rename.py <new_package_name> [--dry-run]

The new name is a lowercase Python identifier, such as ``acme_agents``: lowercase
letters and digits in words joined by single underscores, starting with a letter. It
must not be a Python keyword or the name of a standard-library module.

The script replaces each ``langgraph_postgres_starter`` with the new name, and each
``langgraph-postgres-starter`` (the distribution, the image, the Kubernetes names) with
the new name in hyphens (``acme-agents``), in every text file of the repository: src/,
tests/, scripts/, docs/, deploy/, .github/, pyproject.toml, uv.lock, langgraph.json, the
Makefile, the Dockerfile, compose.yaml, AGENTS.md, README.md and conftest.py among them.
Then it renames each folder and file whose name holds either name, such as
``src/langgraph_postgres_starter/``. It skips ``.git/``, ``.venv/``, ``.superpowers/``,
caches, symbolic links and binary files, and ``LICENSE``, whose copyright notice the MIT
License asks every copy to keep.

``uv.lock`` needs no new lock: the project's name is its only change there, and ``uv lock
--check`` accepts it. With ``--dry-run`` the script lists what it would change and
changes nothing. Afterwards run ``uv sync --all-groups``, which installs the renamed
package, and ``make fmt``, because the new name changes line lengths; then ``make lint
typecheck test``, and shorten by hand any line that the new name made too long. This
script renames its own names too, so it can run again later.
"""

import argparse
import keyword
import os
import re
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path

PROG = "rename.py"
OLD_PACKAGE = "langgraph_postgres_starter"
OLD_DIST = "langgraph-postgres-starter"

# Lowercase words of letters and digits, joined by single underscores: also a valid
# distribution name once the underscores become hyphens.
_NAME = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*")
SKIP_DIRS = frozenset(
    {
        ".git",
        ".venv",
        ".superpowers",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
        ".langgraph_api",
        ".benchmarks",
        "htmlcov",
        "node_modules",
    }
)
SKIP_FILES = frozenset({"LICENSE"})


def name_error(name: str) -> str | None:
    """Return why ``name`` cannot be the new package name, or None when it can."""
    if not _NAME.fullmatch(name):
        return (
            "use lowercase letters and digits in words joined by single underscores, "
            "starting with a letter, such as acme_agents"
        )
    if keyword.iskeyword(name):
        return "it is a Python keyword"
    if name in sys.stdlib_module_names:
        return "it is the name of a standard-library module"
    if name == OLD_PACKAGE:
        return "it is the package's name already"
    return None


def _walk(root: Path) -> Iterator[Path]:
    """Yield every folder and regular file below ``root``, parents first, in name order."""
    for folder, subfolders, files in os.walk(root):
        here = Path(folder)
        subfolders[:] = sorted(
            name for name in subfolders if name not in SKIP_DIRS and not (here / name).is_symlink()
        )
        for name in subfolders:
            yield here / name
        for name in sorted(files):
            path = here / name
            if name not in SKIP_FILES and not path.is_symlink():
                yield path


def _replace(text: str, new_package: str) -> str:
    return text.replace(OLD_PACKAGE, new_package).replace(OLD_DIST, new_package.replace("_", "-"))


def plan(root: Path, new_package: str) -> tuple[dict[Path, bytes], list[tuple[Path, Path]]]:
    """Return the new content of each file to change, and each path to rename (old, new).

    A file is text when it is UTF-8 with no NUL byte; the others are left alone. The
    renames are deepest first, so each one's parent still has its old name.
    """
    contents: dict[Path, bytes] = {}
    renames: list[tuple[Path, Path]] = []
    for path in _walk(root):
        if path.is_file():
            data = path.read_bytes()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                continue
            if "\0" not in text and (new_text := _replace(text, new_package)) != text:
                contents[path] = new_text.encode("utf-8")
        if (new_name := _replace(path.name, new_package)) != path.name:
            renames.append((path, path.with_name(new_name)))
    renames.sort(key=lambda pair: len(pair[0].parts), reverse=True)
    return contents, renames


def _show(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=f"Rename the package {OLD_PACKAGE} (and {OLD_DIST}) everywhere in the "
        "repository. Run it from the repository root.",
    )
    parser.add_argument("name", help="the new package name, such as acme_agents")
    parser.add_argument("--dry-run", action="store_true", help="list the changes, make none")
    args = parser.parse_args(argv)
    name: str = args.name
    if (error := name_error(name)) is not None:
        print(f"{PROG}: {name!r} is not a valid package name: {error}", file=sys.stderr)
        return 2
    root = Path.cwd()
    if not ((root / "pyproject.toml").is_file() and (root / "src" / OLD_PACKAGE).is_dir()):
        print(
            f"{PROG}: run this from the repository root, the folder that holds "
            f"pyproject.toml and src/{OLD_PACKAGE}/ (not {root})",
            file=sys.stderr,
        )
        return 2

    contents, renames = plan(root, name)
    verb = "Would edit" if args.dry_run else "Edited"
    print(f"{verb} {len(contents)} files:")
    for path, data in contents.items():
        if not args.dry_run:
            path.write_bytes(data)
        print(f"  {_show(root, path)}")
    # Show each rename with its parents' new names too, as the tree will look.
    verb = "Would rename" if args.dry_run else "Renamed"
    print(f"{verb} {len(renames)} paths:")
    for old, new in renames:
        if not args.dry_run:
            old.rename(new)
        print(f"  {_show(root, old)} -> {_replace(_show(root, old), name)}")
    if not args.dry_run:
        print(
            "Next: uv sync --all-groups, which installs the renamed package; make fmt, "
            "because the new name changes line lengths; then make lint typecheck test."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
