# Feedback for stpa-workbench RFC 0001 and artifact schema v0, from a real analysis

From using an adaptation of schema v0 as the source of otel-chdb's STPA (losses, hazards, system
constraints, the control structure with each controller's process model and control algorithm, UCAs, loss
scenarios, STPA-Sec, STPA-Teaming, derived requirements, 57 CAST rows, three extension analyses): 222
records and one structure document, exported to strict v0 and validated with `stpawb validate` (280
records, 0 diagnostics, 0 findings; the pilot of 95 records gave 10 findings, "controller without any
responsibility").
Details and the full audit: [README.md](README.md) §2 and §5. Kept in this repository; not sent to
the workbench repository.

## 1. Normalization: v0 stores several facts twice

The owner's criterion is one fact, one home; inverses and derived values computed; any remaining copy
a checked cache. v0 has these copies (README §2 has the table, W1–W23):

- **Kind three or four times**: `kind:`, the id prefix, the folder, the schema chosen. Drop `kind:`;
  the id prefix is the kind.
- **Title twice**: `title:` and the body's `# heading`. Drop the heading rule; renderers add it.
- **`schema:` in every record** as well as in the manifest.
- **Derivable fields stored**: `uca.controller` (= `action.from`), `ucca.contributions[].controller`,
  `scenario.hazards` (= its findings' hazards), `constraint.implementation` (= its mechanisms'), the
  claim's `evidence` status (= its evidence results), `proposal.changes` (= the semantic diff, rule
  10), `disposition.subject` (= `path[-1]`), `state: superseded` (= a newer `supersedes`),
  `baseline.subject_commit` (= its decision's).
- **Both directions of one link**: `claim.evidence_records` ↔ `evidence.supports/challenges`
  (checked), and `assumption.qualifies` ↔ `uca/ucca.assumptions` (**not checked**: they can disagree
  silently).
- **Two homes for one assumption**: `feedback.freshness` and `process_model[].freshness_assumption`
  for the same feedback.
- **One component, two identities** (A02): our components are controllers and controlled processes
  at once (the query service, the alerting engine, the UI, the edge collectors). One `component`
  record with computed roles was simpler to write and to render; the v0 export computes the split.
- **Paths with meaning**: `views[].derived_from` lists folders, against §2's "nothing may infer from a
  path".

## 2. The control structure as one document, instead of action and feedback records

v0 keeps each control action and each feedback as a record of its own (`action`, `feedback`, with `from`
and `to`), next to separate `controller` and `process` records. The owner's requirement was a structure
where redundant links are impossible, and records cannot give that: two action records can describe the
same link, a feedback record can point the wrong way, and a component that is both controller and
controlled needs two identities. Our replacement (README §5.1):

- **One document** (`structure.yaml`) is the only place links exist. Nodes are references to
  `controller` and `controlled_process` records; links are one map entry per ordered pair,
  `"<upper> -> <lower>"`, with `control:` and `feedback:` maps of labelled entries. Direction is not a
  field (control runs down the key's arrow, feedback back up), and a strict parser refuses duplicate keys,
  so a duplicate pair, action or feedback cannot be written at all. The upper end must be a controller
  (the node's id is typed), so a controlled process issuing control cannot be written either. What can be
  written but is wrong (the pair in both orders, cycles, a self link, a dangling node) is checked.
- **Diagrams are views** of the one structure (a subset of nodes and their placement), so two diagrams
  cannot disagree about a shared link.
- **Records cite links by path**, `<upper>-><lower>/<control|feedback>/<key>` (a UCA's `action`, a loss
  scenario's flawed `feedback`); the controller of a UCA is the path's upper end, derived.
- **One record per controller, also when it is controlled** (A02's split is computed by the export).
- **The controller's internals live in its record**: `process_model` variables (name, meaning, the
  feedback paths that update them, or a `source` sentence when no feedback does) and `control_algorithm`
  rules (a condition over the variables, the control paths they issue). Checked both ways: every feedback
  updates a variable, every control action is issued by a rule. v0's `process_model[]` requires a
  `source_feedback`, so a belief from the controller's own clock or from configuration cannot be stated,
  and v0 has no field for the control algorithm at all (we export it under `ext`).

We propose this shape for the workbench: it removes v0's `action`/`feedback` records and the
controller/process split, and turns three of the validator's rules into things that cannot be written.

## 3. Kinds and fields our analysis needed

| Need | v0 | Proposal (overlay in [workbench-v0-overlay/](workbench-v0-overlay/)) |
| --- | --- | --- |
| Hazard refinement (H-E1 ⊂ H-3, H-6) | none | `hazard.refines` |
| System-level constraints from hazards (STPA Handbook step 1) | `constraint.addresses` takes only scenarios and UCAs | `constraint.hazards` |
| Losses without stakeholder records | `stakeholders` required | optional |
| Derived requirements, from anything (constraints, LS, UCA, SEC, TM, CAST), with a priority | none (a `constraint` is close but narrower) | `requirement` kind |
| CAST analyses (issue, found by, hazard, controller + flawed process model, why it made sense, fix, lesson) | "incidents … not in v0" | `incident` kind; its prose cites labels, and the cited hazards are derived by scanning |
| STPA-Sec and STPA-Teaming rows | fit `scenario` with `factors` and `ext` | as is |
| Long titles (CAST issues are 300+ characters) | `title` ≤ 200 | no limit, or a separate statement field |
| Diagram layout hints (rows, columns, spans) | `views[]` closed (`additionalProperties: false`) | a `layout` object per view, or view files of their own |
| A view's short labels (the drawn "insert, repair" vs the action "Insert a statement") | labels only as `UCA-3`-style aliases | a structure entry's `label` is the drawn text, its `title` the full name; an edge draws its members' labels |
| A loss scenario whose cause is a feedback flaw, not a UCA (our former UCA-10, UCA-12) | `scenario.findings` must be UCAs | `feedback` (paths) with a `factor` (feedback missing, wrong, late) |
| A mechanism that settles a loss scenario; a requirement's implementation | `mechanism.implements` takes constraints only | also requirements and scenarios; a scenario's status is derived from them |
| A label that moved (a UCA that became a scenario) | none | `formerly` on the record that holds it now; the label stays resolvable |

## 4. Tooling

- `validate.py` hard-codes the kinds (`ANY_KIND`) and the reference targets (`REFS`), so a new schema
  file is ignored and an extension kind is `unknown-kind` even when its schema validates. Read both
  from the schemas (e.g. an `x-ref-kinds` keyword on each reference). Our shim patches two constants.
- `"controller without any responsibility"` is enforced by the validator but is not among the spec's
  rules 1–13; either add it to the spec or drop it. (Our export now derives one responsibility per
  controller from its description and scope.)
- The generated-diagram check assumes one Mermaid edge per record labelled with the title. A readable
  diagram groups records (four actions between two boxes are one line) and uses short labels; the
  check could compare the grouped edges instead.
- Citation labels need uniqueness; `label` is "non-identifying" in v0, yet documents, code, tests and
  commits cite ours (H-2, R-S3, CAST-50). A project-level rule "labels matching PATTERN are unique"
  would cover it.
- Prose mentions of labels are links: checking them (an unknown label, like our H-8, is an authoring
  error) avoids storing the same link as prose and as a ref list.
- A renderer for the control structure in the Handbook's style (levels, control left of feedback,
  loops around the side, a key) was the owner's main wish; ours is in `tools/` (deterministic,
  golden-tested) and could move to the workbench.
