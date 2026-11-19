# Python worker dependencies

## RB-301 | Missing Python module

ModuleNotFoundError or a "No module named" message while loading task code indicates that the requested Python module is unavailable on the failing worker's import path. Inspect that worker image's installed package inventory and dependency declarations for the module.

## RB-302 | Incompatible Python dependency

An ImportError such as "cannot import name" indicates the installed dependency may lack the symbol or API version the task expects. A compiled extension that fails to initialize with an unresolved shared library symbol or an ABI mismatch is another import-time dependency failure. Inspect the failing worker image's package versions and the task's declared requirements.
