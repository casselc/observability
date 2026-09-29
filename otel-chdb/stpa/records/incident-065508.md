---
id: incident-065508
label: CAST-8
batch: initial
title: RefCell borrow held across an await
found_by: Clippy in the CI spike
hazard: H-8
controller: Consumer runtime
why: Borrow looked short-lived
fix: Read first, borrow after; lint made fatal
lesson: Turn known-dangerous lints into errors
state: accepted
---
