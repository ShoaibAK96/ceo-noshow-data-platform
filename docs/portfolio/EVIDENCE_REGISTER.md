# Public artifact inventory

This page indexes the public files in this repository. It contains no private-source filenames, internal evidence records, deployment identifiers or original-source manifest.

| Public path | Purpose | Validation boundary |
|---|---|---|
| src/transfer.py | Sanitized file-transfer example | Local synthetic checks only |
| office-scripts/prepare_workbook.ts | Sanitized workbook-preparation example | ExcelScript runtime execution not performed locally |
| fabric/loader_v3.py | Sanitized notebook example | Python syntax checked; Fabric runtime execution not performed locally |
| fabric/precheck_v3.py | Sanitized precheck example | Python syntax checked; Fabric runtime execution not performed locally |
| config/fabric.example.json | Generalized configuration example | Requires development-environment validation |
| src/reference_demo.py | Reconstructed local demonstration | Synthetic data only |
| tests/test_reference.py | Local reference tests | Does not certify a production environment |
| docs/images/ | Public architecture illustrations | Illustrative documentation; no production screenshots |

No production throughput, cost-saving or availability figure is asserted. Private operational evidence is outside this repository and is not reproduced here.
