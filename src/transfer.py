# Sanitized derivative of an available original artifact; deployment is not certified.
from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"


def load_config() -> Dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"noshow_transfer_{datetime.now():%Y-%m-%d}.log"

    logger = logging.getLogger("noshow_transfer")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS file_transfer_state (
                source_file_name TEXT NOT NULL,
                source_file_size INTEGER NOT NULL,
                source_modified_ns INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                processed_at_utc TEXT NOT NULL,
                PRIMARY KEY (
                    source_file_name,
                    source_file_size,
                    source_modified_ns
                )
            )
            """
        )
        connection.commit()


def already_copied(database_path: Path, source: Path) -> bool:
    stat = source.stat()
    with sqlite3.connect(str(database_path)) as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM file_transfer_state
            WHERE source_file_name = ?
              AND source_file_size = ?
              AND source_modified_ns = ?
              AND status = 'COPIED'
            LIMIT 1
            """,
            (source.name, stat.st_size, stat.st_mtime_ns),
        ).fetchone()
    return row is not None


def record_state(
    database_path: Path,
    source: Path,
    destination: Path,
    status: str,
    message: str,
) -> None:
    stat = source.stat()
    processed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with sqlite3.connect(str(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO file_transfer_state (
                source_file_name,
                source_file_size,
                source_modified_ns,
                source_path,
                destination_path,
                status,
                message,
                processed_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                source_file_name,
                source_file_size,
                source_modified_ns
            ) DO UPDATE SET
                destination_path = excluded.destination_path,
                status = excluded.status,
                message = excluded.message,
                processed_at_utc = excluded.processed_at_utc
            """,
            (
                source.name,
                stat.st_size,
                stat.st_mtime_ns,
                str(source),
                str(destination),
                status,
                message,
                processed_at,
            ),
        )
        connection.commit()


def append_transfer_log(
    log_dir: Path,
    status: str,
    source: Path,
    destination: Path,
    message: str,
) -> None:
    csv_path = log_dir / "noshow_files_copied.csv"
    new_file = not csv_path.exists()
    size = source.stat().st_size if source.exists() else ""

    with csv_path.open("a", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        if new_file:
            writer.writerow(
                [
                    "PROCESSED_TIMESTAMP",
                    "STATUS",
                    "FILE_NAME",
                    "FILE_SIZE_BYTES",
                    "SOURCE_PATH",
                    "DESTINATION_PATH",
                    "MESSAGE",
                ]
            )
        writer.writerow(
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                status,
                source.name,
                size,
                str(source),
                str(destination),
                message,
            ]
        )


def eligible_files(source_dir: Path, config: Dict[str, Any]) -> List[Path]:
    contains = str(config["file_name_contains"]).casefold()
    extensions = {str(ext).casefold() for ext in config["allowed_extensions"]}
    ignored_prefixes = tuple(str(item).casefold() for item in config["ignore_prefixes"])

    files: List[Path] = []
    for item in source_dir.iterdir():
        if not item.is_file():
            continue
        folded_name = item.name.casefold()
        if folded_name.startswith(ignored_prefixes):
            continue
        if item.suffix.casefold() not in extensions:
            continue
        if contains not in folded_name:
            continue
        files.append(item)

    return sorted(files, key=lambda path: (path.stat().st_mtime_ns, path.name.casefold()))


def copy_file(
    source: Path,
    destination_dir: Path,
    buffer_mb: int,
    overwrite_existing: bool,
) -> Path:
    destination = destination_dir / source.name
    partial = destination_dir / f"{source.name}.part"
    before = source.stat()

    if destination.exists():
        if not overwrite_existing:
            if destination.stat().st_size == before.st_size:
                raise FileExistsError("Destination already contains a file with the same name and size")
            raise FileExistsError("Destination contains the same filename with a different size")
        destination.unlink()

    if partial.exists():
        partial.unlink()

    try:
        with source.open("rb") as source_handle, partial.open("xb") as destination_handle:
            shutil.copyfileobj(
                source_handle,
                destination_handle,
                length=max(1, int(buffer_mb)) * 1024 * 1024,
            )
            destination_handle.flush()
            os.fsync(destination_handle.fileno())

        after = source.stat()
        if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
            raise RuntimeError("Source file changed while it was being copied")
        if partial.stat().st_size != before.st_size:
            raise RuntimeError("Copied file size does not match the source file size")

        os.rename(str(partial), str(destination))
        if destination.stat().st_size != before.st_size:
            raise RuntimeError("Final destination size verification failed")
        return destination
    except Exception:
        if partial.exists():
            partial.unlink()
        raise


def main() -> int:
    config = load_config()
    source_dir = Path(config["source_path"])
    destination_dir = Path(config["destination_path"])
    log_dir = Path(config["logs_path"])
    database_path = Path(config["state_database"])
    logger = setup_logging(log_dir)

    logger.info("RUN_START | Project=%s", config["project_name"])
    logger.info("SOURCE | %s", source_dir)
    logger.info("DESTINATION | %s", destination_dir)

    if not source_dir.is_dir():
        logger.error("SOURCE_UNAVAILABLE | %s", source_dir)
        return 1
    if not destination_dir.is_dir():
        logger.error("DESTINATION_UNAVAILABLE | %s", destination_dir)
        return 1

    initialize_database(database_path)
    minimum_age_seconds = int(config["minimum_file_age_minutes"]) * 60
    current_time = time.time()
    copied = skipped = failed = 0

    candidates = eligible_files(source_dir, config)
    logger.info("FILES_FOUND | Count=%d", len(candidates))

    for source in candidates:
        destination = destination_dir / source.name
        try:
            age_seconds = current_time - source.stat().st_mtime
            if age_seconds < minimum_age_seconds:
                skipped += 1
                logger.info(
                    "SKIPPED_NOT_READY | File=%s | AgeSeconds=%d",
                    source.name,
                    int(age_seconds),
                )
                continue

            if already_copied(database_path, source):
                skipped += 1
                logger.info("SKIPPED_ALREADY_COPIED | File=%s", source.name)
                continue

            final_path = copy_file(
                source,
                destination_dir,
                int(config["copy_buffer_mb"]),
                bool(config["overwrite_existing"]),
            )
            message = "File copied and size verified successfully"
            record_state(database_path, source, final_path, "COPIED", message)
            append_transfer_log(log_dir, "COPIED", source, final_path, message)
            copied += 1
            logger.info(
                "COPIED | FILE_NAME=%s | Bytes=%d | Destination=%s",
                source.name,
                source.stat().st_size,
                final_path,
            )
        except FileExistsError as exc:
            message = str(exc)
            record_state(database_path, source, destination, "SKIPPED", message)
            append_transfer_log(log_dir, "SKIPPED", source, destination, message)
            skipped += 1
            logger.warning("SKIPPED_DESTINATION_EXISTS | File=%s | %s", source.name, message)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            try:
                record_state(database_path, source, destination, "FAILED", message)
                append_transfer_log(log_dir, "FAILED", source, destination, message)
            except Exception as log_error:
                logger.error("STATE_LOG_FAILURE | File=%s | %s", source.name, log_error)
            failed += 1
            logger.exception("FAILED | FILE_NAME=%s | %s", source.name, message)

    logger.info(
        "RUN_END | Copied=%d | Skipped=%d | Failed=%d",
        copied,
        skipped,
        failed,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logging.basicConfig(level=logging.ERROR)
        logging.exception("FATAL_ERROR")
        sys.exit(2)
