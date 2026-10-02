---
id: incident-2ebe58
label: CAST-86
batch: models-and-verification-2026-09-29
title: "The watermark-history change replaced `consume admit`'s report text (\"no history\") with history fields, but the query-integration scale-down test in another module still asserted the old text, so nightly #63 went red; D35 and FORMAT.md §3.1 also still said the consumer keeps no history"
found_by: "Nightly #63, query-integration job"
hazard: "Verification false failure; stale contract documentation (H-5 class: docs describing completeness wrongly)"
controller: "The change's author: \"the report text is only checked by the Rust tests I ran\""
why: "The integration test lives in a different module, outside the local run"
fix: "The test asserts the new history fields; D35 and FORMAT §3.1 updated (7893472); nightly #65 green"
lesson: "After changing an output contract, grep every module and doc for its consumers before pushing (CAST-46 and CAST-61 again)"
state: accepted
---
