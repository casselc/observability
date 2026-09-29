---
id: sec-a088b3
label: SEC-7
title: Log injection makes the UI or proxy run unintended SQL
unsafe: Rewrite proxy or query service executes attacker text
hazards: [hazard-d14498]
mitigation: Parse and rebuild SQL from an AST; never splice user text; read-only ClickHouse users
state: accepted
---
