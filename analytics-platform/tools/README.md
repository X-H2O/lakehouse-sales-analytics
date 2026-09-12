# Local Tooling Notes

This directory contains a tiny Windows helper used by local Spark jobs.

- `hadoop/bin/winutils.exe`: local Windows compatibility stub for Spark.
- `winutils-stub/WinutilsStub.cs`: source code for the stub.

The helper is included only to make the local Windows demo reproducible. It is not part of the data-platform architecture.
