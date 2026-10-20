# LinkedIn Project Post

A reliable CEO dashboard starts well before the first visual—with the files that feed it.

As part of the CEO Dashboard work at Aga Khan University Hospital, I built connected NoShow data-processing components across Python, Power Automate, Office Scripts and Microsoft Fabric.

The workflow brings together three engineering stages:
• Python collects new operational files while preserving the source and recording transfer history.
• Power Automate and Office Scripts validate incoming files and turn pipe-delimited records into structured Excel workbooks.
• A metadata-driven PySpark framework validates workbooks and loads typed Delta tables with lineage, operational logs and archival controls.

The most interesting work was handling what happens between those stages: repeated deliveries, incomplete files, mixed date formats and interrupted runs.

I implemented file-level duplicate guards, exact-row deduplication, explicit type mappings and recovery logic scoped to an individual file and run. I also separated the upstream reporting-date rule from Fabric's responsibility to validate the date supplied in the workbook.

A key lesson: recovery needs its own design. Loading data, moving a file and writing a success log are separate operations. A good pipeline must make an inconsistent state visible and give the operator a safe way to reconcile it.

This work demonstrates how Python automation, Microsoft 365 workflows and a Lakehouse ingestion framework can work together for executive healthcare reporting.

The portfolio uses synthetic examples and sanitized documentation. It distinguishes the implemented components from integration hardening that still needs deployment evidence.

#DataEngineering #MicrosoftFabric #Python #PowerAutomate #PySpark #DeltaLake #HealthcareAnalytics #BusinessIntelligence
