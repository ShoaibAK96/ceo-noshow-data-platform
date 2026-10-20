# Data contract
The upstream source uses an envelope with a C column record and D data records. Required 15 fields are listed in office-scripts/prepare_workbook.ts; report_date makes 16 workbook columns. Metadata maps them to typed Delta columns; 3 audit fields produce 19 business-table columns. Three target tables serve data, file history and errors. These are schema counts, not scale metrics.

Upstream original behavior: derive reporting date from filename. Fabric: trust the supplied workbook business date after conversion, independently of filename and appointment timestamps. Identifier fields remain strings; advance is integer; advance_adjust is string. Business timestamps use day-first parsing, report_date month-first where applicable. Blank optional timestamps remain null. Exact row equality is evaluated before audit fields are added.

Demo differences: uses ISO strings and SQLite/CSV; strict synthetic filename validation and duplicate-header checks are reference hardening, not claims about the original Office Script. Does not validate trailer totals. Sanitized config is the closest mapping reference; production compatibility still needs runtime validation.
