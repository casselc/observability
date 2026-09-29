#!/usr/bin/env python3
"""Run the stpa-workbench reference validator (`stpawb validate`) over the v0 export, with this
overlay's schemas. Usage: stpawb_overlay.py WORKBENCH_SCHEMAS_V0_DIR EXPORT_DIR

Two things the overlay needs that v0's validator cannot take as data, which is the point of the
workbench feedback (workbench-feedback.md): the set of kinds is a constant (ANY_KIND), and so is
the field -> target-kind table (REFS). This shim extends both before calling the validator.
"""
import os, shutil, sys, tempfile
from stpa_workbench_tools import validate as v, cli

here = os.path.dirname(os.path.abspath(__file__))
wb_schemas, export = sys.argv[1], sys.argv[2]
merged = tempfile.mkdtemp(prefix="stp-schemas-")
for d in (wb_schemas, here):
    for f in os.listdir(d):
        if f.endswith(".schema.json"):
            shutil.copyfile(os.path.join(d, f), os.path.join(merged, f))
v.ANY_KIND |= {"requirement", "incident"}
v.REFS["refines"] = {"hazard"}
v.REFS["derived_from"] = {"constraint", "scenario", "uca", "ucca", "hazard", "incident", "requirement"}
try:
    rc = cli.main(["--schemas", merged, "validate", export])
finally:
    shutil.rmtree(merged)
sys.exit(rc)
