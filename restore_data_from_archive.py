from __future__ import annotations

from pathlib import Path


ARCHIVE_DIR = Path("archive")
DATA_DIR = Path("data")


def restore_files() -> None:
    if not ARCHIVE_DIR.exists():
        print("Archive folder does not exist. Nothing to restore.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    restored = 0

    for src in sorted(ARCHIVE_DIR.glob("*.backup")):
        original_name = src.name.removesuffix(".backup")
        dst = DATA_DIR / original_name
        if dst.exists():
            dst.unlink()
        src.rename(dst)
        restored += 1

    print(f"Restored {restored} file(s) from archive to data.")


if __name__ == "__main__":
    restore_files()
