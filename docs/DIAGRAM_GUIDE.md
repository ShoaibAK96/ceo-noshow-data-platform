# Reading the diagrams
The summary shows the three connected workstreams. The detailed diagram follows file execution order, not development dates. Teal means implementation supported by original code or documented flow evidence; amber marks missing deployment evidence or future work; red shows exceptions and manual recovery. Mermaid uses a dotted server-to-Fabric edge for the unverified integration. No historical load or locked branch is evidenced.

The reporting date is derived upstream by the original Office Script. Fabric independently validates the supplied workbook date. Duplicate controls are distinct: transfer metadata, daily output history, successful Fabric filenames and exact rows within a workbook. None proves global business-key uniqueness or exactly-once distributed execution.

Regenerate PNGs with Python 3 and Pillow: python scripts_render_diagrams.py. Font dependency: DejaVu Sans under /usr/share/fonts/truetype/dejavu. Fixed geometry, palette and text make the rendering repeatable. Rendering source is separate from the production pipeline.
