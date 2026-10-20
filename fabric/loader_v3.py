# Sanitized derivative of an available original artifact; deployment is not certified.
from __future__ import annotations


# ==============================================================================================================
# SECTION 01: 01_Project_Setup_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 01 - Project Setup

Defines shared runtime context, path helpers, logging, and common utilities.
"""

import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from pyspark.sql import SparkSession

try:
    from notebookutils import mssparkutils
except Exception:
    try:
        import mssparkutils
    except Exception:
        mssparkutils = None


DEFAULT_PROJECT = {
    "name": "NoShow_Loader_V3",
    "lakehouse": "EXAMPLE_LAKEHOUSE",
    "root": "/lakehouse/default/Files/Executive_Reporting/NoShow",
    "config_file": "fabric.example.json",
    "default_schema": "analytics",
    "default_file_log_table": "fact_noshow_file_log",
    "default_error_log_table": "fact_noshow_error_log",
}


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def configure_logger(project_name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(project_name)
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    return logger


def normalize_header(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def resolve_project_path(root: str, configured_path: Optional[str], fallback: str) -> str:
    """Resolve short, Lakehouse-relative, mounted, or ABFS paths safely."""
    candidate = str(configured_path or fallback).strip()
    normalized = candidate.replace("\\", "/")

    if normalized.startswith(("abfss://", "abfs://", "file:/")):
        return candidate
    if os.path.isabs(candidate):
        return os.path.normpath(candidate)
    if normalized.casefold().startswith(("files/", "tables/")):
        return os.path.normpath(os.path.join("/lakehouse/default", normalized))

    default_prefix = "/lakehouse/default/"
    normalized_root = root.replace("\\", "/").rstrip("/")
    root_relative = (
        normalized_root[len(default_prefix):]
        if normalized_root.casefold().startswith(default_prefix.casefold())
        else ""
    )
    if root_relative and normalized.casefold().startswith(root_relative.casefold() + "/"):
        return os.path.normpath(os.path.join("/lakehouse/default", normalized))

    return os.path.normpath(os.path.join(root, candidate))


def path_exists(path: str) -> bool:
    if os.path.exists(path):
        return True
    if mssparkutils is not None:
        try:
            return bool(mssparkutils.fs.exists(path))
        except Exception:
            return False
    return False


def ensure_directory(path: str) -> None:
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return
    except Exception:
        pass
    if mssparkutils is not None:
        mssparkutils.fs.mkdirs(path)
        return
    raise OSError(f"Unable to create directory: {path}")


def create_project_context(
    spark_session: Optional[SparkSession] = None,
    project_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    project = dict(DEFAULT_PROJECT)
    if project_overrides:
        project.update(project_overrides)

    spark = spark_session or SparkSession.builder.getOrCreate()
    logger = configure_logger(project["name"])
    root = project["root"]

    return {
        "project": project,
        "spark": spark,
        "logger": logger,
        "config": None,
        "metadata": {},
        "paths": {
            "root": root,
            "config": os.path.join(root, "Config", project["config_file"]),
            "incoming": os.path.join(root, "Incoming"),
            "archive": os.path.join(root, "Archive"),
        },
        "run": {
            "id": str(uuid.uuid4()),
            "start_time": utc_now_naive(),
            "end_time": None,
            "status": "INITIALIZED",
        },
        "state": {
            "files_found": 0,
            "files_processed": 0,
            "files_succeeded": 0,
            "files_failed": 0,
            "files_skipped": 0,
            "tables_loaded": [],
            "rows_loaded": 0,
            "warnings": [],
            "errors": [],
            "file_results": [],
        },
    }


def initialize_project_directories(context: Dict[str, Any]) -> Dict[str, Any]:
    ensure_directory(context["paths"]["root"])
    ensure_directory(os.path.dirname(context["paths"]["config"]))
    ensure_directory(context["paths"]["incoming"])
    ensure_directory(context["paths"]["archive"])
    return context


def print_project_banner(context: Dict[str, Any]) -> None:
    print("=" * 100)
    print(context["project"]["name"])
    print("=" * 100)
    print(f"Run ID          : {context['run']['id']}")
    print(f"Run start (UTC) : {context['run']['start_time']}")
    print(f"Lakehouse       : {context['project']['lakehouse']}")
    print(f"Root            : {context['paths']['root']}")
    print(f"Configuration   : {context['paths']['config']}")
    print(f"Incoming        : {context['paths']['incoming']}")
    print(f"Archive         : {context['paths']['archive']}")
    print("=" * 100)


# ==============================================================================================================
# SECTION 02: 02_Configuration_Manager_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 02 - Configuration Manager

Loads, standardizes, validates, and stores the JSON configuration.
"""

import json
from typing import Any, Dict, List

SUPPORTED_LOAD_MODES = {"append", "overwrite", "replace_file"}
REQUIRED_TABLE_KEYS = {"sheet", "table", "columns"}
REQUIRED_COLUMN_KEYS = {"source", "target", "type"}


def _require_non_empty_string(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{field_name}' must be a non-empty string.")


def load_json_file(path: str) -> Dict[str, Any]:
    if not path_exists(path):
        raise FileNotFoundError(f"Configuration file was not found: {path}")
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def validate_column_configuration(column_cfg: Dict[str, Any], table_name: str) -> None:
    missing = REQUIRED_COLUMN_KEYS.difference(column_cfg)
    if missing:
        raise ValueError(f"Table '{table_name}' column definition missing: {sorted(missing)}")

    _require_non_empty_string(column_cfg["source"], "column.source")
    _require_non_empty_string(column_cfg["target"], "column.target")
    _require_non_empty_string(column_cfg["type"], "column.type")

    if "date_format" in column_cfg and column_cfg["date_format"] is not None:
        _require_non_empty_string(column_cfg["date_format"], "column.date_format")
    if "dayfirst" in column_cfg and not isinstance(column_cfg["dayfirst"], bool):
        raise TypeError("column.dayfirst must be true or false when provided.")


def validate_table_configuration(table_cfg: Dict[str, Any]) -> None:
    missing = REQUIRED_TABLE_KEYS.difference(table_cfg)
    if missing:
        raise ValueError(f"Table configuration missing required keys: {sorted(missing)}")

    _require_non_empty_string(table_cfg["sheet"], "table.sheet")
    _require_non_empty_string(table_cfg["table"], "table.table")

    columns = table_cfg["columns"]
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"Table '{table_cfg['table']}' must contain at least one column.")

    for column_cfg in columns:
        if not isinstance(column_cfg, dict):
            raise TypeError(f"Every column for '{table_cfg['table']}' must be an object.")
        validate_column_configuration(column_cfg, table_cfg["table"])

    source_columns = [normalize_header(col["source"]) for col in columns]
    target_columns = [col["target"].strip() for col in columns]

    if len(source_columns) != len(set(source_columns)):
        raise ValueError(f"Table '{table_cfg['table']}' has duplicate normalized source columns.")
    if len(target_columns) != len(set(target_columns)):
        raise ValueError(f"Table '{table_cfg['table']}' has duplicate target columns.")

    required = table_cfg.get("required_columns", source_columns)
    if not isinstance(required, list):
        raise TypeError(f"required_columns must be a list for '{table_cfg['table']}'.")

    configured_sources = set(source_columns)
    unknown_required = {
        normalize_header(col) for col in required if normalize_header(col) not in configured_sources
    }
    if unknown_required:
        raise ValueError(
            f"Table '{table_cfg['table']}' required columns are not configured: "
            f"{sorted(unknown_required)}"
        )

    load_mode = str(table_cfg.get("load_mode", "append")).lower()
    if load_mode not in SUPPORTED_LOAD_MODES:
        raise ValueError(
            f"Unsupported load_mode '{load_mode}'. Supported: {sorted(SUPPORTED_LOAD_MODES)}"
        )


def standardize_configuration(raw_config: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_config, dict):
        raise TypeError("The root JSON value must be an object.")

    settings = dict(raw_config.get("settings") or {})
    tables = raw_config.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("Configuration must contain a non-empty 'tables' list.")

    settings.setdefault("schema", context["project"]["default_schema"])
    settings.setdefault("incoming_folder", "Incoming")
    settings.setdefault("archive_folder", "Archive")
    settings.setdefault("config_version", "3.1-noshow")
    settings.setdefault("audit_columns", ["source_file", "run_id", "load_timestamp"])
    settings.setdefault("strict_data_types", True)
    settings.setdefault("continue_on_error", True)
    settings.setdefault("archive_on_success", True)
    settings.setdefault("file_log_table", context["project"]["default_file_log_table"])
    settings.setdefault("error_log_table", context["project"]["default_error_log_table"])

    standardized: List[Dict[str, Any]] = []
    for raw_table in tables:
        table_cfg = dict(raw_table)
        table_cfg.setdefault("enabled", True)
        table_cfg.setdefault("load_mode", "append")
        table_cfg.setdefault("header_row", 0)
        table_cfg.setdefault("skip_blank_rows", True)
        table_cfg.setdefault("allow_empty", False)
        table_cfg.setdefault(
            "required_columns",
            [col["source"] for col in table_cfg.get("columns", [])],
        )
        standardized.append(table_cfg)

    return {"settings": settings, "tables": standardized}


def validate_configuration(config: Dict[str, Any]) -> None:
    settings = config["settings"]
    _require_non_empty_string(settings.get("schema"), "settings.schema")
    _require_non_empty_string(settings.get("incoming_folder"), "settings.incoming_folder")
    _require_non_empty_string(settings.get("archive_folder"), "settings.archive_folder")
    _require_non_empty_string(settings.get("file_log_table"), "settings.file_log_table")
    _require_non_empty_string(settings.get("error_log_table"), "settings.error_log_table")

    audit_columns = settings.get("audit_columns")
    if not isinstance(audit_columns, list) or not audit_columns:
        raise ValueError("settings.audit_columns must be a non-empty list.")
    if not {"source_file", "run_id", "load_timestamp"}.issubset(set(audit_columns)):
        raise ValueError("audit_columns must include source_file, run_id, and load_timestamp.")

    seen_sheets = set()
    seen_tables = set()
    for table_cfg in config["tables"]:
        validate_table_configuration(table_cfg)
        if not table_cfg.get("enabled", True):
            continue
        sheet_key = normalize_header(table_cfg["sheet"]).casefold()
        table_key = table_cfg["table"].strip().casefold()
        if sheet_key in seen_sheets:
            raise ValueError(f"Duplicate enabled sheet: {table_cfg['sheet']}")
        if table_key in seen_tables:
            raise ValueError(f"Duplicate enabled destination table: {table_cfg['table']}")
        seen_sheets.add(sheet_key)
        seen_tables.add(table_key)

    if not seen_tables:
        raise ValueError("At least one table configuration must be enabled.")


def update_context_paths_from_configuration(context: Dict[str, Any]) -> None:
    settings = context["config"]["settings"]
    root = context["paths"]["root"]
    context["paths"]["incoming"] = resolve_project_path(
        root, settings.get("incoming_folder"), "Incoming"
    )
    context["paths"]["archive"] = resolve_project_path(
        root, settings.get("archive_folder"), "Archive"
    )


def initialize_configuration(context: Dict[str, Any]) -> Dict[str, Any]:
    raw = load_json_file(context["paths"]["config"])
    config = standardize_configuration(raw, context)
    validate_configuration(config)
    context["config"] = config
    update_context_paths_from_configuration(context)
    initialize_project_directories(context)

    schema_name = config["settings"]["schema"]
    context["spark"].sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")
    context["logger"].info(
        "NOSHOW configuration version %s loaded with %s table definition(s).",
        config["settings"]["config_version"], len(config["tables"])
    )
    return context


def print_configuration_summary(context: Dict[str, Any]) -> None:
    settings = context["config"]["settings"]
    print("=" * 100)
    print("SECTION 02 - NOSHOW CONFIGURATION SUMMARY")
    print("=" * 100)
    print(f"Version          : {settings['config_version']}")
    print(f"Schema           : {settings['schema']}")
    print(f"Incoming         : {context['paths']['incoming']}")
    print(f"Archive          : {context['paths']['archive']}")
    print(f"File log         : {settings['schema']}.{settings['file_log_table']}")
    print(f"Error log        : {settings['schema']}.{settings['error_log_table']}")
    print(f"Strict types     : {settings['strict_data_types']}")
    print(f"Continue on error: {settings['continue_on_error']}")
    for table_cfg in context["config"]["tables"]:
        if table_cfg.get("enabled", True):
            print(f"Mapping          : {table_cfg['sheet']} -> {settings['schema']}.{table_cfg['table']}")
    print("=" * 100)


# ==============================================================================================================
# SECTION 03: 03_Metadata_Engine_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 03 - Metadata Engine

Builds reusable mappings and explicit Spark ingestion/final schemas.
"""

import re
from typing import Any, Dict, List

from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

TYPE_ALIASES = {
    "str": "string", "text": "string", "int": "integer", "bigint": "long",
    "float": "double", "datetime": "timestamp", "bool": "boolean",
}


def canonical_type_name(configured_type: str) -> str:
    normalized = str(configured_type).strip().lower()
    return TYPE_ALIASES.get(normalized, normalized)


def spark_type_for(configured_type: str, *, ingestion_stage: bool):
    type_name = canonical_type_name(configured_type)
    if type_name == "string": return StringType()
    if type_name == "integer": return IntegerType()
    if type_name == "long": return LongType()
    if type_name == "double": return DoubleType()
    if type_name == "boolean": return BooleanType()
    if type_name == "timestamp": return TimestampType()
    if type_name == "date": return TimestampType() if ingestion_stage else DateType()
    if type_name.startswith("decimal"):
        match = re.fullmatch(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", type_name)
        if not match:
            raise ValueError(f"Decimal type must use decimal(precision,scale): {configured_type}")
        precision, scale = map(int, match.groups())
        return DecimalType(precision, scale)
    raise ValueError(f"Unsupported configured type: '{configured_type}'.")


def audit_column_types() -> Dict[str, Any]:
    return {
        "source_file": StringType(),
        "run_id": StringType(),
        "load_timestamp": TimestampType(),
    }


def build_schema(table_cfg: Dict[str, Any], audit_columns: List[str], *, ingestion_stage: bool) -> StructType:
    fields = [
        StructField(
            col["target"],
            spark_type_for(col["type"], ingestion_stage=ingestion_stage),
            True,
        )
        for col in table_cfg["columns"]
    ]
    audit_types = audit_column_types()
    for audit in audit_columns:
        if audit not in audit_types:
            raise ValueError(f"Unsupported audit column: {audit}")
        fields.append(StructField(audit, audit_types[audit], True))
    return StructType(fields)


def build_table_metadata(context: Dict[str, Any], table_cfg: Dict[str, Any]) -> Dict[str, Any]:
    audit_columns = context["config"]["settings"]["audit_columns"]
    return {
        "enabled": table_cfg.get("enabled", True),
        "sheet": table_cfg["sheet"],
        "normalized_sheet": normalize_header(table_cfg["sheet"]),
        "table": table_cfg["table"],
        "qualified_table": f"{context['config']['settings']['schema']}.{table_cfg['table']}",
        "load_mode": str(table_cfg.get("load_mode", "append")).lower(),
        "header_row": int(table_cfg.get("header_row", 0)),
        "skip_blank_rows": bool(table_cfg.get("skip_blank_rows", True)),
        "allow_empty": bool(table_cfg.get("allow_empty", False)),
        "required_columns": [normalize_header(c) for c in table_cfg.get("required_columns", [])],
        "source_columns": [normalize_header(c["source"]) for c in table_cfg["columns"]],
        "target_columns": [c["target"] for c in table_cfg["columns"]],
        "audit_columns": list(audit_columns),
        "all_target_columns": [c["target"] for c in table_cfg["columns"]] + list(audit_columns),
        "rename_map": {normalize_header(c["source"]): c["target"] for c in table_cfg["columns"]},
        "type_map": {c["target"]: canonical_type_name(c["type"]) for c in table_cfg["columns"]},
        "parse_options_map": {
            c["target"]: {
                "date_format": c.get("date_format"),
                "dayfirst": c.get("dayfirst"),
            }
            for c in table_cfg["columns"]
        },
        "ingestion_schema": build_schema(table_cfg, audit_columns, ingestion_stage=True),
        "final_schema": build_schema(table_cfg, audit_columns, ingestion_stage=False),
        "raw_config": table_cfg,
    }


def initialize_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
    if context.get("config") is None:
        raise ValueError("Configuration is not initialized. Run Section 02 first.")

    metadata: Dict[str, Dict[str, Any]] = {}
    for table_cfg in context["config"]["tables"]:
        if not table_cfg.get("enabled", True):
            continue
        item = build_table_metadata(context, table_cfg)
        key = item["normalized_sheet"].casefold()
        if key in metadata:
            raise ValueError(f"Duplicate normalized sheet metadata: {item['sheet']}")
        metadata[key] = item

    if not metadata:
        raise ValueError("No enabled metadata was generated.")
    context["metadata"] = metadata
    context["logger"].info("Metadata initialized for %s table(s).", len(metadata))
    return context


def print_metadata_summary(context: Dict[str, Any]) -> None:
    print("=" * 100)
    print("SECTION 03 - NOSHOW METADATA SUMMARY")
    print("=" * 100)
    for metadata in context["metadata"].values():
        print(f"Sheet            : {metadata['sheet']}")
        print(f"Destination      : {metadata['qualified_table']}")
        print(f"Load mode        : {metadata['load_mode']}")
        print(f"Ingestion schema : {metadata['ingestion_schema'].simpleString()}")
        print(f"Final schema     : {metadata['final_schema'].simpleString()}")
        for source, target in metadata["rename_map"].items():
            opts = metadata["parse_options_map"][target]
            extra = ""
            if metadata["type_map"][target] in {"date", "timestamp"}:
                extra = f", dayfirst={opts.get('dayfirst')}, format={opts.get('date_format')}"
            print(f"  {source} -> {target} ({metadata['type_map'][target]}{extra})")
        print("-" * 100)


# ==============================================================================================================
# SECTION 04: 04_Workbook_Reader_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 04 - Workbook Reader

Discovers Excel files and reads only configured worksheets. Because only
NoshowData is configured, the empty Sheet1 worksheet is ignored automatically.
"""

import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

SUPPORTED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}


def _fabric_list_paths(folder: str) -> List[str]:
    if mssparkutils is None:
        return []
    paths: List[str] = []
    for item in mssparkutils.fs.ls(folder):
        if not bool(getattr(item, "isDir", False)):
            paths.append(getattr(item, "path", str(item)))
    return paths


def list_incoming_excel_files(context: Dict[str, Any]) -> List[str]:
    incoming = context["paths"]["incoming"]
    if os.path.isdir(incoming):
        discovered = [
            os.path.join(incoming, name)
            for name in os.listdir(incoming)
            if os.path.isfile(os.path.join(incoming, name))
        ]
    else:
        discovered = _fabric_list_paths(incoming)

    valid = []
    for path in discovered:
        name = os.path.basename(path)
        if name.startswith("~$"):
            continue
        if Path(name).suffix.lower() not in SUPPORTED_EXCEL_EXTENSIONS:
            continue
        valid.append(path)

    valid.sort(key=lambda p: os.path.basename(p).casefold())
    context["state"]["files_found"] = len(valid)
    context["logger"].info("Discovered %s Excel file(s) in %s.", len(valid), incoming)
    return valid


def normalize_dataframe_headers(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    columns = [normalize_header(c) for c in normalized.columns]
    duplicates = {c for c in columns if columns.count(c) > 1}
    if duplicates:
        raise ValueError(f"Duplicate columns after header normalization: {sorted(duplicates)}")
    normalized.columns = columns
    return normalized


def match_actual_sheet_name(available_sheets: List[str], configured_sheet_name: str) -> str:
    lookup = {normalize_header(s).casefold(): s for s in available_sheets}
    key = normalize_header(configured_sheet_name).casefold()
    if key not in lookup:
        raise KeyError(
            f"Configured sheet '{configured_sheet_name}' was not found. "
            f"Available sheets: {available_sheets}"
        )
    return lookup[key]


def read_workbook_file(context: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    file_name = os.path.basename(file_path)
    context["logger"].info("Reading NOSHOW workbook: %s", file_name)

    try:
        excel_file = pd.ExcelFile(file_path)
    except Exception as exc:
        raise RuntimeError(f"Unable to open workbook '{file_name}': {exc}") from exc

    try:
        workbook_sheets: Dict[str, pd.DataFrame] = {}
        sheet_name_map: Dict[str, str] = {}
        for metadata in context["metadata"].values():
            actual_sheet = match_actual_sheet_name(excel_file.sheet_names, metadata["sheet"])
            dataframe = pd.read_excel(
                excel_file,
                sheet_name=actual_sheet,
                header=metadata["header_row"],
                engine=None,
            )
            dataframe = normalize_dataframe_headers(dataframe)
            key = metadata["normalized_sheet"].casefold()
            workbook_sheets[key] = dataframe
            sheet_name_map[key] = actual_sheet

        return {
            "file_path": file_path,
            "file_name": file_name,
            "available_sheets": list(excel_file.sheet_names),
            "sheet_name_map": sheet_name_map,
            "sheets": workbook_sheets,
        }
    finally:
        try:
            excel_file.close()
        except Exception:
            pass


# ==============================================================================================================
# SECTION 05: 05_Validation_Engine_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 05 - Validation Engine

Validates workbook structure before transformation or Delta writes.
"""

from typing import Any, Dict, List

import pandas as pd


class WorkbookValidationError(ValueError):
    pass


def validate_sheet_dataframe(dataframe: pd.DataFrame, metadata: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    existing = set(dataframe.columns)

    missing_required = [c for c in metadata["required_columns"] if c not in existing]
    missing_configured = [c for c in metadata["source_columns"] if c not in existing]
    if missing_required:
        errors.append(f"Missing required columns: {missing_required}")
    if missing_configured:
        errors.append(f"Missing configured columns: {missing_configured}")

    if dataframe.empty:
        if metadata.get("allow_empty", False):
            warnings.append("The configured worksheet contains no data rows.")
        else:
            errors.append("The configured worksheet contains no data rows.")

    unnamed = [c for c in dataframe.columns if not c or c.lower().startswith("unnamed:")]
    if unnamed:
        warnings.append(f"Unnamed columns will be ignored: {unnamed}")

    extra = [c for c in dataframe.columns if c not in metadata["source_columns"]]
    if extra:
        warnings.append(f"Extra columns will be ignored: {extra}")

    return {
        "sheet": metadata["sheet"],
        "table": metadata["qualified_table"],
        "row_count": int(len(dataframe)),
        "errors": errors,
        "warnings": warnings,
        "is_valid": not errors,
    }


def validate_workbook(context: Dict[str, Any], workbook: Dict[str, Any]) -> Dict[str, Any]:
    sheet_reports = []
    workbook_errors: List[str] = []
    workbook_warnings: List[str] = []

    for sheet_key, metadata in context["metadata"].items():
        if sheet_key not in workbook["sheets"]:
            workbook_errors.append(f"Configured sheet '{metadata['sheet']}' was not loaded.")
            continue
        report = validate_sheet_dataframe(workbook["sheets"][sheet_key], metadata)
        sheet_reports.append(report)
        workbook_errors.extend(f"{metadata['sheet']}: {m}" for m in report["errors"])
        workbook_warnings.extend(f"{metadata['sheet']}: {m}" for m in report["warnings"])

    context["state"]["warnings"].extend(workbook_warnings)
    result = {
        "file_name": workbook["file_name"],
        "is_valid": not workbook_errors,
        "errors": workbook_errors,
        "warnings": workbook_warnings,
        "sheets": sheet_reports,
    }
    if workbook_errors:
        raise WorkbookValidationError(
            f"Workbook '{workbook['file_name']}' failed validation: " + " | ".join(workbook_errors)
        )

    context["logger"].info("Workbook validation passed: %s", workbook["file_name"])
    return result


# ==============================================================================================================
# SECTION 06: 06_Transformation_Engine_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 06 - Transformation Engine

Selects configured columns, removes blank rows, performs field-specific date
parsing, converts types, removes exact duplicate business rows, and adds audit
columns.
"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def _non_blank_mask(series: pd.Series) -> pd.Series:
    return series.notna() & series.astype("string").str.strip().ne("")


def _convert_boolean(series: pd.Series) -> pd.Series:
    truthy = {"true", "t", "yes", "y", "1"}
    falsy = {"false", "f", "no", "n", "0"}
    def convert(value: Any):
        if pd.isna(value) or str(value).strip() == "":
            return pd.NA
        normalized = str(value).strip().casefold()
        if normalized in truthy: return True
        if normalized in falsy: return False
        return pd.NA
    return series.map(convert).astype("boolean")


def _parse_datetime_value(
    value: Any,
    *,
    date_format: Optional[str],
    dayfirst: Optional[bool],
):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return pd.NaT
    if isinstance(value, (pd.Timestamp, datetime, date, np.datetime64)):
        return pd.Timestamp(value)

    # If Excel exposes a raw serial number rather than a datetime object,
    # convert it using the standard Excel 1900-date-system epoch.
    if isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, bool):
        try:
            return pd.Timestamp("1899-12-30") + pd.to_timedelta(float(value), unit="D")
        except Exception:
            return pd.NaT

    text = str(value).strip()
    if not text:
        return pd.NaT

    kwargs = {"errors": "coerce"}
    if date_format:
        kwargs["format"] = date_format
    elif dayfirst is not None:
        kwargs["dayfirst"] = bool(dayfirst)
    return pd.to_datetime(text, **kwargs)


def convert_series(
    series: pd.Series,
    configured_type: str,
    parse_options: Optional[Dict[str, Any]] = None,
) -> pd.Series:
    type_name = canonical_type_name(configured_type)
    parse_options = parse_options or {}

    if type_name == "string":
        def to_clean_string(value: Any):
            if value is None or (not isinstance(value, str) and pd.isna(value)):
                return pd.NA
            if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
                return str(int(value))
            if isinstance(value, (float, np.floating)):
                number = float(value)
                if np.isnan(number):
                    return pd.NA
                if number.is_integer():
                    return str(int(number))
            text = str(value).strip()
            return text if text else pd.NA
        return series.map(to_clean_string).astype("string")

    if type_name in {"integer", "long"}:
        numeric = pd.to_numeric(series, errors="coerce")
        non_integral = numeric.notna() & ((numeric % 1).abs() > 1e-12)
        numeric = numeric.mask(non_integral)
        return numeric.astype("Int64")

    if type_name == "double":
        return pd.to_numeric(series, errors="coerce").astype("Float64")

    if type_name in {"date", "timestamp"}:
        converted = series.map(
            lambda value: _parse_datetime_value(
                value,
                date_format=parse_options.get("date_format"),
                dayfirst=parse_options.get("dayfirst"),
            )
        )
        return pd.to_datetime(converted, errors="coerce")

    if type_name == "boolean":
        return _convert_boolean(series)

    if type_name.startswith("decimal"):
        def to_decimal(value: Any):
            if pd.isna(value) or str(value).strip() == "":
                return None
            try:
                return Decimal(str(value).strip())
            except (InvalidOperation, ValueError):
                return None
        return series.map(to_decimal)

    raise ValueError(f"Unsupported conversion type: '{configured_type}'.")


def detect_conversion_failures(source_series: pd.Series, converted_series: pd.Series) -> int:
    return int((_non_blank_mask(source_series) & converted_series.isna()).sum())


def remove_blank_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    cleaned = dataframe.copy()
    for column in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[column]) or pd.api.types.is_string_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].replace(r"^\s*$", pd.NA, regex=True)
    return cleaned.dropna(how="all")


def transform_sheet(
    context: Dict[str, Any],
    dataframe: pd.DataFrame,
    metadata: Dict[str, Any],
    source_file: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    selected = dataframe.loc[:, metadata["source_columns"]].copy()
    input_rows = len(selected)

    if metadata["skip_blank_rows"]:
        selected = remove_blank_rows(selected)
    rows_after_blank_removal = len(selected)

    selected = selected.rename(columns=metadata["rename_map"])
    conversion_failures: Dict[str, int] = {}

    for target_column, configured_type in metadata["type_map"].items():
        original = selected[target_column].copy()
        converted = convert_series(
            original,
            configured_type,
            metadata["parse_options_map"].get(target_column),
        )
        failures = detect_conversion_failures(original, converted)
        if failures:
            conversion_failures[target_column] = failures
        selected[target_column] = converted

    if conversion_failures:
        message = f"{metadata['sheet']} has data type conversion failures: {conversion_failures}"
        if context["config"]["settings"]["strict_data_types"]:
            raise ValueError(message)
        context["state"]["warnings"].append(message)
        context["logger"].warning(message)

    before_duplicates = len(selected)
    selected = selected.drop_duplicates().reset_index(drop=True)
    duplicates_removed = before_duplicates - len(selected)

    selected["source_file"] = source_file
    selected["run_id"] = context["run"]["id"]
    selected["load_timestamp"] = pd.Timestamp(utc_now_naive())
    selected = selected.loc[:, metadata["all_target_columns"]]

    summary = {
        "sheet": metadata["sheet"],
        "table": metadata["qualified_table"],
        "input_rows": int(input_rows),
        "output_rows": int(len(selected)),
        "blank_rows_removed": int(input_rows - rows_after_blank_removal),
        "duplicates_removed": int(duplicates_removed),
        "conversion_failures": conversion_failures,
    }
    return selected, summary


def transform_workbook(context: Dict[str, Any], workbook: Dict[str, Any]) -> Dict[str, Any]:
    transformed_sheets: Dict[str, pd.DataFrame] = {}
    summaries: List[Dict[str, Any]] = []
    for sheet_key, metadata in context["metadata"].items():
        transformed, summary = transform_sheet(
            context, workbook["sheets"][sheet_key], metadata, workbook["file_name"]
        )
        transformed_sheets[sheet_key] = transformed
        summaries.append(summary)

    context["logger"].info("Workbook transformation completed: %s", workbook["file_name"])
    return {
        "file_path": workbook["file_path"],
        "file_name": workbook["file_name"],
        "sheets": transformed_sheets,
        "summaries": summaries,
    }


# ==============================================================================================================
# SECTION 07: 07_Spark_Delta_Engine_Noshow_V3_Rollback.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 07 - Spark and Delta Engine with Rollback
"""

import math
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


def python_scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        number = float(value)
        return None if math.isnan(number) else number
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def pandas_records(dataframe: pd.DataFrame) -> List[tuple]:
    return [
        tuple(python_scalar(value) for value in row)
        for row in dataframe.itertuples(index=False, name=None)
    ]


def pandas_to_spark(
    context: Dict[str, Any],
    dataframe: pd.DataFrame,
    metadata: Dict[str, Any],
) -> SparkDataFrame:
    return context["spark"].createDataFrame(
        pandas_records(dataframe),
        schema=metadata["ingestion_schema"],
    )


def apply_final_types(spark_dataframe: SparkDataFrame, metadata: Dict[str, Any]) -> SparkDataFrame:
    result = spark_dataframe
    for target_column, configured_type in metadata["type_map"].items():
        type_name = canonical_type_name(configured_type)
        if type_name == "date":
            result = result.withColumn(target_column, F.to_date(F.col(target_column)))
        elif type_name == "timestamp":
            result = result.withColumn(target_column, F.col(target_column).cast("timestamp"))
        elif type_name == "integer":
            result = result.withColumn(target_column, F.col(target_column).cast("int"))
        elif type_name == "long":
            result = result.withColumn(target_column, F.col(target_column).cast("bigint"))
        elif type_name == "double":
            result = result.withColumn(target_column, F.col(target_column).cast("double"))
        elif type_name == "boolean":
            result = result.withColumn(target_column, F.col(target_column).cast("boolean"))
        elif type_name == "string":
            result = result.withColumn(target_column, F.col(target_column).cast("string"))
        elif type_name.startswith("decimal"):
            result = result.withColumn(target_column, F.col(target_column).cast(type_name))
        else:
            raise ValueError(f"Unsupported final Spark type: '{configured_type}'.")
    return result.select(*metadata["all_target_columns"])


def table_exists(context: Dict[str, Any], qualified_table: str) -> bool:
    try:
        return bool(context["spark"].catalog.tableExists(qualified_table))
    except Exception:
        schema_name, table_name = qualified_table.split(".", 1)
        return bool(context["spark"].catalog.tableExists(schema_name, table_name))


def escape_sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def write_delta_table(
    context: Dict[str, Any],
    spark_dataframe: SparkDataFrame,
    metadata: Dict[str, Any],
    source_file: str,
) -> Dict[str, Any]:
    qualified_table = metadata["qualified_table"]
    load_mode = metadata["load_mode"]
    row_count = spark_dataframe.count()

    if row_count == 0:
        return {
            "table": qualified_table,
            "load_mode": load_mode,
            "rows_loaded": 0,
            "status": "SKIPPED_EMPTY",
        }

    if load_mode == "replace_file" and table_exists(context, qualified_table):
        escaped_source = escape_sql_literal(source_file)
        context["spark"].sql(
            f"DELETE FROM {qualified_table} WHERE source_file = '{escaped_source}'"
        )

    writer = spark_dataframe.write.format("delta")
    if load_mode == "overwrite":
        writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(qualified_table)
    else:
        writer.mode("append").option("mergeSchema", "false").saveAsTable(qualified_table)

    return {
        "table": qualified_table,
        "load_mode": load_mode,
        "rows_loaded": int(row_count),
        "status": "LOADED",
    }


def rollback_file_load(context: Dict[str, Any], source_file: str, run_id: str) -> List[str]:
    """Delete and verify current-attempt rows using source_file + run_id."""
    escaped_source = escape_sql_literal(source_file)
    escaped_run = escape_sql_literal(run_id)
    rolled_back: List[str] = []

    for metadata in context["metadata"].values():
        table_name = metadata["qualified_table"]
        if not table_exists(context, table_name):
            continue

        columns = set(context["spark"].table(table_name).columns)
        if not {"source_file", "run_id"}.issubset(columns):
            raise RuntimeError(
                f"Rollback cannot safely operate on {table_name}; "
                "source_file and run_id audit columns are required."
            )

        context["spark"].sql(
            f"""
            DELETE FROM {table_name}
            WHERE source_file = '{escaped_source}'
              AND run_id = '{escaped_run}'
            """
        )

        remaining = (
            context["spark"].table(table_name)
            .filter((F.col("source_file") == source_file) & (F.col("run_id") == run_id))
            .limit(1)
            .count()
        )
        if remaining:
            raise RuntimeError(f"Rollback verification failed for {table_name}.")

        rolled_back.append(table_name)
        context["logger"].warning(
            "Rolled back source_file='%s', run_id='%s' from %s.",
            source_file, run_id, table_name,
        )

    return rolled_back


def load_transformed_workbook(
    context: Dict[str, Any],
    transformed_workbook: Dict[str, Any],
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for sheet_key, pandas_dataframe in transformed_workbook["sheets"].items():
        metadata = context["metadata"][sheet_key]
        spark_dataframe = pandas_to_spark(context, pandas_dataframe, metadata)
        final_dataframe = apply_final_types(spark_dataframe, metadata)
        results.append(
            write_delta_table(
                context,
                final_dataframe,
                metadata,
                transformed_workbook["file_name"],
            )
        )
    context["logger"].info(
        "Delta loading completed for workbook: %s", transformed_workbook["file_name"]
    )
    return results


# ==============================================================================================================
# SECTION 08: 08_Logging_Archive_Engine_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 08 - Logging and Archive Engine

Creates the NOSHOW file-log and error-log tables when missing, validates their
schemas when they already exist, writes logs, and archives successful files.
"""

import os
import shutil
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

FILE_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("tables_loaded", IntegerType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("archive_path", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])

ERROR_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("source_file", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("stack_trace", StringType(), True),
    StructField("error_timestamp", TimestampType(), False),
])


def logging_table_name(context: Dict[str, Any], setting_name: str) -> str:
    settings = context["config"]["settings"]
    return f"{settings['schema']}.{settings[setting_name]}"


def spark_table_exists(context: Dict[str, Any], qualified_table: str) -> bool:
    try:
        return bool(context["spark"].catalog.tableExists(qualified_table))
    except Exception:
        schema_name, table_name = qualified_table.split(".", 1)
        return bool(context["spark"].catalog.tableExists(schema_name, table_name))


def _schema_signature(schema: StructType):
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


def validate_existing_log_schema(
    context: Dict[str, Any], qualified_table: str, expected_schema: StructType
) -> None:
    actual_schema = context["spark"].table(qualified_table).schema
    if _schema_signature(actual_schema) != _schema_signature(expected_schema):
        raise RuntimeError(
            f"Existing logging table {qualified_table} has an incompatible schema. "
            f"Expected {_schema_signature(expected_schema)}, "
            f"actual {_schema_signature(actual_schema)}. "
            "Drop/migrate the old logging table before running this V3 loader."
        )


def create_delta_table_if_not_exists(
    context: Dict[str, Any], qualified_table: str, schema: StructType
) -> None:
    if spark_table_exists(context, qualified_table):
        validate_existing_log_schema(context, qualified_table, schema)
        return

    empty_dataframe = context["spark"].createDataFrame([], schema=schema)
    empty_dataframe.write.format("delta").mode("ignore").saveAsTable(qualified_table)
    context["logger"].info("Created logging Delta table: %s", qualified_table)


def ensure_logging_tables(context: Dict[str, Any]) -> Dict[str, str]:
    file_log = logging_table_name(context, "file_log_table")
    error_log = logging_table_name(context, "error_log_table")
    create_delta_table_if_not_exists(context, file_log, FILE_LOG_SCHEMA)
    create_delta_table_if_not_exists(context, error_log, ERROR_LOG_SCHEMA)
    return {"file_log_table": file_log, "error_log_table": error_log}


def append_log_record(
    context: Dict[str, Any], record: tuple, schema: StructType, qualified_table: str
) -> None:
    dataframe = context["spark"].createDataFrame([record], schema=schema)
    dataframe.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(qualified_table)


def write_file_log(
    context: Dict[str, Any], *, source_file: str, status: str,
    file_start_time: datetime, file_end_time: datetime,
    tables_loaded: int, rows_loaded: int,
    archive_path: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    ensure_logging_tables(context)
    duration = (file_end_time - file_start_time).total_seconds()
    record = (
        context["run"]["id"], source_file, status,
        file_start_time, file_end_time, float(duration),
        int(tables_loaded), int(rows_loaded), archive_path,
        error_message, utc_now_naive(),
    )
    append_log_record(
        context, record, FILE_LOG_SCHEMA,
        logging_table_name(context, "file_log_table")
    )


def write_error_log(
    context: Dict[str, Any], *, source_file: Optional[str], stage: str,
    exception: Exception, stack_trace_override: Optional[str] = None,
) -> None:
    ensure_logging_tables(context)
    stack_trace = stack_trace_override if stack_trace_override is not None else traceback.format_exc()
    record = (
        context["run"]["id"], source_file, stage,
        type(exception).__name__, str(exception), stack_trace, utc_now_naive(),
    )
    append_log_record(
        context, record, ERROR_LOG_SCHEMA,
        logging_table_name(context, "error_log_table")
    )


def build_archive_path(context: Dict[str, Any], source_file_path: str) -> str:
    source_name = os.path.basename(source_file_path)
    p = Path(source_name)
    stamp = utc_now_naive().strftime("%Y%m%dT%H%M%S%f")
    return os.path.join(context["paths"]["archive"], f"{p.stem}_{stamp}{p.suffix}")


def archive_source_file(context: Dict[str, Any], source_file_path: str) -> str:
    destination = build_archive_path(context, source_file_path)
    ensure_directory(context["paths"]["archive"])
    try:
        shutil.move(source_file_path, destination)
    except Exception:
        if mssparkutils is None:
            raise
        mssparkutils.fs.mv(source_file_path, destination, True)

    if not path_exists(destination):
        raise RuntimeError(f"Archive verification failed; destination was not found: {destination}")

    context["logger"].info("Archived %s to %s.", os.path.basename(source_file_path), destination)
    return destination


def record_error_state(
    context: Dict[str, Any], *, source_file: Optional[str], stage: str, exception: Exception
) -> None:
    context["state"]["errors"].append({
        "source_file": source_file,
        "stage": stage,
        "error_type": type(exception).__name__,
        "error_message": str(exception),
        "timestamp": utc_now_naive().isoformat(),
    })


def finalize_run_state(context: Dict[str, Any]) -> Dict[str, Any]:
    context["run"]["end_time"] = utc_now_naive()
    if context["state"]["files_failed"] > 0:
        context["run"]["status"] = (
            "PARTIAL_SUCCESS" if context["state"]["files_succeeded"] > 0 else "FAILED"
        )
    else:
        context["run"]["status"] = "SUCCESS"
    return context


def print_run_summary(context: Dict[str, Any]) -> None:
    duration = (context["run"]["end_time"] - context["run"]["start_time"]).total_seconds()
    print("=" * 100)
    print("CEO NOSHOW LOADER V3 - RUN SUMMARY")
    print("=" * 100)
    print(f"Run ID           : {context['run']['id']}")
    print(f"Status           : {context['run']['status']}")
    print(f"Files found      : {context['state']['files_found']}")
    print(f"Files processed  : {context['state']['files_processed']}")
    print(f"Files succeeded  : {context['state']['files_succeeded']}")
    print(f"Files failed     : {context['state']['files_failed']}")
    print(f"Files skipped    : {context['state']['files_skipped']}")
    print(f"Rows loaded      : {context['state']['rows_loaded']:,}")
    print(f"Warnings         : {len(context['state']['warnings'])}")
    print(f"Errors           : {len(context['state']['errors'])}")
    print(f"Duration seconds : {duration:.2f}")
    print("=" * 100)


# ==============================================================================================================
# SECTION 09: 09_Pipeline_Orchestrator_Noshow_V3_Rollback.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 09 - Rollback-Aware Pipeline Orchestrator

Adds authoritative duplicate checking, an ambiguous-existing-data guard,
workbook-level rollback, logging, and sequential multi-file processing.
"""

import os
import traceback
from typing import Any, Dict, List

from pyspark.sql import functions as F


def file_has_success_log(context: Dict[str, Any], source_file: str) -> bool:
    file_log_table = logging_table_name(context, "file_log_table")
    if not spark_table_exists(context, file_log_table):
        return False
    log_df = context["spark"].table(file_log_table)
    columns = set(log_df.columns)
    filename_column = "source_file" if "source_file" in columns else "file_name" if "file_name" in columns else None
    if filename_column is None or "status" not in columns:
        raise RuntimeError(f"File log {file_log_table} lacks source_file/file_name or status.")
    return bool(
        log_df
        .filter(F.trim(F.col(filename_column)) == source_file)
        .filter(F.upper(F.trim(F.col("status"))) == "SUCCESS")
        .limit(1)
        .count()
    )


def existing_business_rows_for_source(context: Dict[str, Any], source_file: str) -> Dict[str, int]:
    """Detect rows for a filename when no SUCCESS log exists; this prevents blind duplicate appends."""
    found: Dict[str, int] = {}
    for metadata in context["metadata"].values():
        table_name = metadata["qualified_table"]
        if not table_exists(context, table_name):
            continue
        columns = set(context["spark"].table(table_name).columns)
        if "source_file" not in columns:
            continue
        count = (
            context["spark"].table(table_name)
            .filter(F.col("source_file") == source_file)
            .count()
        )
        if count:
            found[table_name] = int(count)
    return found


def handle_already_processed_file(context: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    source_file = os.path.basename(file_path)
    start = utc_now_naive()
    try:
        archive_path = archive_source_file(context, file_path)
        end = utc_now_naive()
        context["state"]["files_skipped"] += 1
        write_file_log(
            context,
            source_file=source_file,
            status="SKIPPED_ALREADY_PROCESSED",
            file_start_time=start,
            file_end_time=end,
            tables_loaded=0,
            rows_loaded=0,
            archive_path=archive_path,
            error_message=None,
        )
        result = {
            "source_file": source_file,
            "status": "SKIPPED_ALREADY_PROCESSED",
            "archive_path": archive_path,
            "rows_loaded": 0,
            "tables_loaded": 0,
        }
        context["state"]["file_results"].append(result)
        return result
    except Exception as exc:
        end = utc_now_naive()
        context["state"]["files_failed"] += 1
        record_error_state(context, source_file=source_file, stage="DUPLICATE_ARCHIVE", exception=exc)
        try:
            write_error_log(
                context, source_file=source_file, stage="DUPLICATE_ARCHIVE",
                exception=exc, stack_trace_override=traceback.format_exc()
            )
            write_file_log(
                context, source_file=source_file, status="FAILED",
                file_start_time=start, file_end_time=end,
                tables_loaded=0, rows_loaded=0, archive_path=None,
                error_message=f"DUPLICATE_ARCHIVE: {exc}"
            )
        except Exception:
            context["logger"].exception("Unable to log duplicate-archive failure for %s", source_file)
        result = {
            "source_file": source_file,
            "status": "FAILED",
            "failed_stage": "DUPLICATE_ARCHIVE",
            "error_message": str(exc),
            "rows_loaded": 0,
            "tables_loaded": 0,
        }
        context["state"]["file_results"].append(result)
        if not context["config"]["settings"]["continue_on_error"]:
            raise
        return result


def process_single_workbook(context: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    source_file = os.path.basename(file_path)
    file_start_time = utc_now_naive()
    stage = "INITIALIZE_FILE"
    load_results: List[Dict[str, Any]] = []
    archive_path = None
    rows_loaded = 0
    tables_loaded = 0
    context["state"]["files_processed"] += 1

    try:
        stage = "READ_WORKBOOK"
        workbook = read_workbook_file(context, file_path)

        stage = "VALIDATE_WORKBOOK"
        validation_report = validate_workbook(context, workbook)

        stage = "TRANSFORM_WORKBOOK"
        transformed_workbook = transform_workbook(context, workbook)

        stage = "LOAD_DELTA"
        load_results = load_transformed_workbook(context, transformed_workbook)
        rows_loaded = sum(r["rows_loaded"] for r in load_results)
        tables_loaded = sum(r["status"] == "LOADED" for r in load_results)

        expected_tables = len(context["metadata"])
        if len(load_results) != expected_tables:
            raise RuntimeError(
                f"Expected {expected_tables} table load result(s), got {len(load_results)}."
            )
        invalid = [r for r in load_results if r["status"] not in {"LOADED", "SKIPPED_EMPTY"}]
        if invalid:
            raise RuntimeError(f"One or more table writes did not complete: {invalid}")

        stage = "ARCHIVE_FILE"
        if context["config"]["settings"]["archive_on_success"]:
            archive_path = archive_source_file(context, file_path)

        context["state"]["rows_loaded"] += rows_loaded
        context["state"]["tables_loaded"].extend(
            r["table"] for r in load_results if r["status"] == "LOADED"
        )
        context["state"]["files_succeeded"] += 1
        file_end_time = utc_now_naive()

        # Logging table is created and schema-validated before processing begins.
        # If this final success log fails, business data remains loaded and the
        # issue is captured as a warning/error-log attempt.
        log_status = "SUCCESS"
        log_error_message = None
        try:
            stage = "WRITE_SUCCESS_LOG"
            write_file_log(
                context,
                source_file=source_file,
                status="SUCCESS",
                file_start_time=file_start_time,
                file_end_time=file_end_time,
                tables_loaded=tables_loaded,
                rows_loaded=rows_loaded,
                archive_path=archive_path,
                error_message=None,
            )
        except Exception as log_exception:
            log_status = "FAILED"
            log_error_message = str(log_exception)
            warning = (
                f"Workbook '{source_file}' loaded and archived successfully, "
                f"but SUCCESS logging failed: {log_exception}"
            )
            context["state"]["warnings"].append(warning)
            context["logger"].exception(warning)
            try:
                write_error_log(
                    context,
                    source_file=source_file,
                    stage="WRITE_SUCCESS_LOG",
                    exception=log_exception,
                    stack_trace_override=traceback.format_exc(),
                )
            except Exception:
                context["logger"].exception("Unable to record SUCCESS-log failure.")

        result = {
            "source_file": source_file,
            "status": "SUCCESS",
            "log_status": log_status,
            "log_error_message": log_error_message,
            "validation": validation_report,
            "loads": load_results,
            "archive_path": archive_path,
            "rows_loaded": rows_loaded,
            "tables_loaded": tables_loaded,
        }
        context["state"]["file_results"].append(result)
        context["logger"].info("Workbook completed successfully: %s", source_file)
        return result

    except Exception as exc:
        original_traceback = traceback.format_exc()
        file_end_time = utc_now_naive()
        rollback_tables: List[str] = []
        rollback_error = None

        if stage in {"LOAD_DELTA", "ARCHIVE_FILE"}:
            try:
                rollback_tables = rollback_file_load(
                    context, source_file=source_file, run_id=context["run"]["id"]
                )
            except Exception as rollback_exception:
                rollback_error = str(rollback_exception)
                rollback_traceback = traceback.format_exc()
                context["state"]["warnings"].append(
                    f"Rollback failed for {source_file}: {rollback_exception}"
                )
                context["logger"].exception("Rollback failed for %s", source_file)
                try:
                    write_error_log(
                        context,
                        source_file=source_file,
                        stage="ROLLBACK",
                        exception=rollback_exception,
                        stack_trace_override=rollback_traceback,
                    )
                except Exception:
                    context["logger"].exception("Unable to log rollback failure.")

        context["state"]["files_failed"] += 1
        record_error_state(context, source_file=source_file, stage=stage, exception=exc)

        try:
            write_error_log(
                context,
                source_file=source_file,
                stage=stage,
                exception=exc,
                stack_trace_override=original_traceback,
            )
        except Exception:
            context["logger"].exception("Unable to write error log for %s", source_file)

        try:
            write_file_log(
                context,
                source_file=source_file,
                status="FAILED",
                file_start_time=file_start_time,
                file_end_time=file_end_time,
                tables_loaded=0,
                rows_loaded=0,
                archive_path=None,
                error_message=f"{stage}: {exc}",
            )
        except Exception:
            context["logger"].exception("Unable to write failure file log for %s", source_file)

        result = {
            "source_file": source_file,
            "status": "FAILED",
            "failed_stage": stage,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "rollback_tables": rollback_tables,
            "rollback_error": rollback_error,
            "archive_path": None,
            "rows_loaded": 0,
            "tables_loaded": 0,
        }
        context["state"]["file_results"].append(result)
        context["logger"].exception("Workbook failed at %s: %s", stage, source_file)
        if not context["config"]["settings"]["continue_on_error"]:
            raise
        return result


def run_incoming_pipeline(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    ensure_logging_tables(context)
    files = list_incoming_excel_files(context)
    if not files:
        context["logger"].warning("No Excel files were found in %s.", context["paths"]["incoming"])
        return []

    results: List[Dict[str, Any]] = []
    for file_path in files:
        source_file = os.path.basename(file_path)

        # Authoritative second duplicate check: safe even if Pre-Section 01 was skipped.
        if file_has_success_log(context, source_file):
            results.append(handle_already_processed_file(context, file_path))
            continue

        # Crash/rollback safety guard. If no SUCCESS exists but rows with this source
        # already exist, do not blindly append another copy. Leave the file in Incoming
        # and require reconciliation of the ambiguous prior state.
        existing_rows = existing_business_rows_for_source(context, source_file)
        if existing_rows:
            exc = RuntimeError(
                f"Idempotency guard blocked '{source_file}'. No SUCCESS log exists, "
                f"but business rows already exist: {existing_rows}. Reconcile/remove "
                "the ambiguous rows before retrying."
            )
            start = utc_now_naive()
            end = utc_now_naive()
            context["state"]["files_failed"] += 1
            record_error_state(context, source_file=source_file, stage="IDEMPOTENCY_GUARD", exception=exc)
            try:
                write_error_log(
                    context, source_file=source_file, stage="IDEMPOTENCY_GUARD",
                    exception=exc, stack_trace_override="Idempotency guard raised before workbook processing."
                )
                write_file_log(
                    context, source_file=source_file, status="FAILED",
                    file_start_time=start, file_end_time=end,
                    tables_loaded=0, rows_loaded=0, archive_path=None,
                    error_message=f"IDEMPOTENCY_GUARD: {exc}"
                )
            except Exception:
                context["logger"].exception("Unable to log idempotency guard for %s", source_file)
            result = {
                "source_file": source_file,
                "status": "FAILED",
                "failed_stage": "IDEMPOTENCY_GUARD",
                "error_message": str(exc),
                "rows_loaded": 0,
                "tables_loaded": 0,
            }
            context["state"]["file_results"].append(result)
            results.append(result)
            if not context["config"]["settings"]["continue_on_error"]:
                raise exc
            continue

        results.append(process_single_workbook(context, file_path))

    return results


# ==============================================================================================================
# SECTION 10: 10_Main_Noshow_V3.py
# ==============================================================================================================

"""
CEO NOSHOW Loader V3
Section 10 - Main Execution

Run after Pre-Section 01 and Sections 01 through 09 have been executed in the
same Fabric notebook session.
"""

from typing import Any, Dict, Optional


def run_noshow_loader_v3(
    project_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context = create_project_context(project_overrides=project_overrides)
    try:
        initialize_project_directories(context)
        print_project_banner(context)

        initialize_configuration(context)
        print_configuration_summary(context)

        initialize_metadata(context)
        print_metadata_summary(context)

        context["run"]["status"] = "RUNNING"
        run_incoming_pipeline(context)

    except Exception as exc:
        record_error_state(
            context,
            source_file=None,
            stage="PIPELINE_INITIALIZATION",
            exception=exc,
        )
        context["state"]["files_failed"] += 1
        context["logger"].exception("CEO NOSHOW V3 pipeline failed during initialization/orchestration.")
        raise
    finally:
        finalize_run_state(context)
        print_run_summary(context)

    return context


CONTEXT = run_noshow_loader_v3()
