# NoShow ingestion — public case study

## Problem
Operational reporting requires structured, consistent input. This portfolio explores a three-stage ingestion architecture: Python file collection, automated workbook preparation, and metadata-driven Lakehouse loading.

## Architecture
Python handles file collection. Power Automate and Office Scripts coordinate workbook preparation. Microsoft Fabric and PySpark provide a typed Delta ingestion layer. The public diagrams illustrate the component boundaries; they are not screenshots of a production deployment.

## Engineering focus
The repository demonstrates explicit data contracts, validation, duplicate handling, lineage and recovery considerations through generalized examples. See the public source, data contract and synthetic tests for reviewable behavior. The local demonstration uses synthetic input and does not connect to organizational systems.

## Provenance and limits
Files are identified as sanitized derivatives or reconstructed portfolio examples in the provenance document. Production configuration, internal operational evidence and maintenance documentation are excluded from this repository. No measured production throughput, financial saving, uptime or clinical outcome is claimed. Passing local tests does not establish live service integration or production acceptance.

## Professional relevance
This project connects workflow automation with data engineering. The reviewable portfolio emphasizes clear interfaces, explicit assumptions and maintainable validation rather than unsupported impact claims.
