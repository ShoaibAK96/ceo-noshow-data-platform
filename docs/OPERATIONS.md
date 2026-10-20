# Operation and recovery
Transfer state uses filename/size/mtime, not a content hash. Preserve it across schedules; enforce one active transfer run. Restore state only after comparing destination and source. Same-name changed files require operator policy.

Power Automate documented intake: properties-only trigger → path-based content → raw preservation → success-history check → template copy → Office Script → server output → archives → cleanup → success log. Invalid/duplicate input is quarantined. No importable flow export is included. Shared Excel log races and locking need acceptance tests; a delay is not a completeness guarantee.

Fabric: copy completed workbooks only, then run notebook. The ready/sent queue is recommended, not confirmed deployed. A filename with a previous success is skipped; orphan business rows block replay. Row deduplication is within each workbook, not across different filenames. Source_file/run_id lineage scopes rollback. Archive/log/Delta are not one distributed transaction. Final success-log failure keeps data loaded and archived; reconcile before replay. If rollback or logging also fails, inspect business rows before corrective action.

No production benchmark is available. The local reference uses a SQLite transaction for demonstration; it does not reproduce distributed Fabric failure semantics. Local CSV export follows the commit and is not atomic with it. No parallel-process guarantee is provided.

Production acceptance: capture original deployment exports and dependency versions, then verify happy path, repeated file, same-day different file, changed same-name input, malformed date, missing column, lock timeout, archive failure, success-log failure and recovery. Record observed latency/counts before publishing numerical claims.
