# Sanitized derivative of an available original artifact; deployment is not certified.
"""
CEO NOSHOW Loader V3
Pre-Section 01 - Previously Processed File Check

Runs before Section 01. Files already recorded as SUCCESS in the NOSHOW
file-log table are moved from Incoming to Archive without being processed.
New files and files with only FAILED history remain in Incoming.

This is a first-line duplicate guard. Section 09 repeats the authoritative
check so the notebook remains safe if this cell is skipped accidentally.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

INCOMING_FOLDER = "/lakehouse/default/Files/Executive_Reporting/NoShow/Incoming"
ARCHIVE_FOLDER = "/lakehouse/default/Files/Executive_Reporting/NoShow/Archive"
FILE_LOG_TABLE = "analytics.fact_noshow_file_log"
SUPPORTED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}

try:
    from notebookutils import mssparkutils
except Exception:
    try:
        import mssparkutils
    except Exception:
        mssparkutils = None


def table_exists(table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        schema_name, short_name = table_name.split(".", 1)
        return bool(spark.catalog.tableExists(schema_name, short_name))


def ensure_archive_folder() -> None:
    try:
        Path(ARCHIVE_FOLDER).mkdir(parents=True, exist_ok=True)
        return
    except Exception:
        pass
    if mssparkutils is not None:
        mssparkutils.fs.mkdirs(ARCHIVE_FOLDER)
        return
    raise OSError(f"Unable to create Archive folder: {ARCHIVE_FOLDER}")


def list_incoming_excel_files() -> List[str]:
    discovered: List[str] = []
    if os.path.isdir(INCOMING_FOLDER):
        for name in os.listdir(INCOMING_FOLDER):
            path = os.path.join(INCOMING_FOLDER, name)
            if os.path.isfile(path):
                discovered.append(path)
    elif mssparkutils is not None:
        for item in mssparkutils.fs.ls(INCOMING_FOLDER):
            if not bool(getattr(item, "isDir", False)):
                discovered.append(getattr(item, "path", str(item)))
    else:
        raise RuntimeError(f"Unable to access Incoming folder: {INCOMING_FOLDER}")

    valid: List[str] = []
    for path in discovered:
        name = os.path.basename(path)
        if name.startswith("~$"):
            continue
        if Path(name).suffix.lower() not in SUPPORTED_EXCEL_EXTENSIONS:
            continue
        valid.append(path)

    return sorted(valid, key=lambda p: os.path.basename(p).casefold())


def successful_source_files() -> Set[str]:
    if not table_exists(FILE_LOG_TABLE):
        print(f"File-log table does not exist yet: {FILE_LOG_TABLE}")
        return set()

    log_df = spark.table(FILE_LOG_TABLE)
    columns = set(log_df.columns)
    if "source_file" in columns:
        filename_column = "source_file"
    elif "file_name" in columns:
        filename_column = "file_name"
    else:
        raise ValueError(
            f"{FILE_LOG_TABLE} contains neither source_file nor legacy file_name."
        )

    if "status" not in columns:
        raise ValueError(f"{FILE_LOG_TABLE} does not contain status.")

    rows = (
        log_df
        .filter(F.upper(F.trim(F.col("status"))) == F.lit("SUCCESS"))
        .filter(F.col(filename_column).isNotNull())
        .select(F.trim(F.col(filename_column)).alias("source_file"))
        .distinct()
        .collect()
    )
    return {row["source_file"] for row in rows if row["source_file"]}


def build_duplicate_archive_path(source_path: str) -> str:
    p = Path(os.path.basename(source_path))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return os.path.join(
        ARCHIVE_FOLDER,
        f"{p.stem}_ALREADY_PROCESSED_{stamp}{p.suffix}",
    )


def move_to_archive(source_path: str, destination_path: str) -> None:
    ensure_archive_folder()
    try:
        shutil.move(source_path, destination_path)
        return
    except Exception as local_error:
        if mssparkutils is None:
            raise RuntimeError(
                f"Unable to archive already-processed file: {source_path}"
            ) from local_error
        try:
            mssparkutils.fs.mv(source_path, destination_path, True)
            return
        except Exception as fabric_error:
            raise RuntimeError(
                f"Unable to move {source_path} to {destination_path}"
            ) from fabric_error


print("=" * 110)
print("PRE-SECTION 01 - NOSHOW PREVIOUSLY PROCESSED FILE CHECK")
print("=" * 110)
print("Incoming :", INCOMING_FOLDER)
print("Archive  :", ARCHIVE_FOLDER)
print("File log :", FILE_LOG_TABLE)
print("=" * 110)

incoming_files = list_incoming_excel_files()
success_files = successful_source_files()
moved = []
retained = []
move_errors = []

for source_path in incoming_files:
    source_file = os.path.basename(source_path)
    if source_file in success_files:
        destination = build_duplicate_archive_path(source_path)
        try:
            move_to_archive(source_path, destination)
            moved.append(source_file)
            print(f"MOVED   : {source_file} -> already SUCCESS")
        except Exception as exc:
            move_errors.append((source_file, str(exc)))
            print(f"ERROR   : {source_file}: {exc}")
    else:
        retained.append(source_file)
        print(f"RETAINED: {source_file} -> no SUCCESS record")

print("=" * 110)
print(f"Excel files found        : {len(incoming_files)}")
print(f"Moved without processing : {len(moved)}")
print(f"Retained for processing  : {len(retained)}")
print(f"Move errors              : {len(move_errors)}")
print("=" * 110)

# Fail safe: never continue into the loader when an already-successful file
# could not be removed from Incoming, because that could cause duplicate data.
if move_errors:
    details = " | ".join(f"{name}: {error}" for name, error in move_errors)
    raise RuntimeError(
        "Pre-processing duplicate archive move failed. Main ingestion is stopped "
        f"to prevent duplicate loading. Details: {details}"
    )
