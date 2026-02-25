from pathlib import PurePosixPath
from typing import Iterable


def _require_py7zr():
    try:
        import py7zr
    except ImportError as exc:
        raise RuntimeError("py7zr is required for .7z input support") from exc
    return py7zr


def is_supported_input(log_path: str) -> bool:
    from pathlib import Path

    path = Path(log_path)
    return path.exists() and (path.is_dir() or path.suffix.lower() == ".7z")


def list_archive_entries(archive_path: str) -> list[str]:
    py7zr = _require_py7zr()
    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        return list(archive.getnames())


def read_selected_entries(archive_path: str, target_filename: str) -> list[tuple[str, bytes]]:
    py7zr = _require_py7zr()
    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        names = list(archive.getnames())
        selected = [name for name in names if PurePosixPath(name).name == target_filename]
        if not selected:
            raise FileNotFoundError(
                f"No {target_filename} found in archive {archive_path}"
            )

        extracted = archive.read(selected)
        if extracted is None:
            raise RuntimeError(f"Failed to read selected entries from {archive_path}")
        result: list[tuple[str, bytes]] = []
        for name in selected:
            data_stream = extracted[name]
            result.append((name, data_stream.read()))

    return result


def iter_selected_file_bytes(log_path: str, target_filename: str) -> Iterable[tuple[str, bytes]]:
    from pathlib import Path
    import os

    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"log path not found: {log_path}")

    if path.is_dir():
        for root, _, files in os.walk(path):
            for filename in files:
                if filename != target_filename:
                    continue
                file_path = Path(root) / filename
                yield (str(file_path), file_path.read_bytes())
        return

    if path.suffix.lower() == ".7z":
        for name, content in read_selected_entries(str(path), target_filename):
            yield (name, content)
        return

    raise ValueError(
        f"Unsupported input {log_path}: only directory or .7z archive is supported"
    )