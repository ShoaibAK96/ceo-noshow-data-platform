# Fabric runtime boundary
The sanitized .py files derive from the available original V3 source; they are not a new production deployment. Run the precheck and loader in notebook cells with an attached default Lakehouse. The loader uses pandas, PySpark, Delta and Fabric notebook utilities. Match library versions to the selected Fabric runtime and validate in a development workspace. No dependency lock was available. Python compilation was checked locally; no Spark/Fabric integration execution was performed.

The final SUCCESS-log failure path intentionally leaves data loaded and the file archived. Inspect logs, business rows and archive state before replay. The compensating rollback does not provide a distributed transaction or concurrency locking.
