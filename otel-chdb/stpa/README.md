# STPA as records (DECISIONS.md D39, accepted)

The STPA of this repository ([STPA.md](../STPA.md) and the STPA sections of
[research/langfuse.md](../research/langfuse.md), [research/entra-ingress.md](../research/entra-ingress.md) and
[research/grants.md](../research/grants.md)) is kept here as data: every STPA fact is one field of one
record, the control structure is one document ([structure.yaml](structure.yaml)), and the tables, the
control-structure diagrams (SVG in the style of the PRD, the PRD's own widget code, Mermaid), one detail
diagram per controller and the label catalogue are generated from them. The tables and diagrams in STPA.md
and the research notes are generated sections between `<!-- stpa:begin NAME -->` and `<!-- stpa:end NAME -->`
markers: edit the records, run `stpa render`, and CI fails if a section was edited by hand or not
re-rendered.

**Status (2026-09-29):** adopted by the owner ("go with your recs, migrate the rest"); every table of
STPA.md is migrated (222 records, §6), and the structure became its own document with the controllers'
process models and control algorithms (§5).

Contents: [1. Criterion](#1-the-criterion-one-fact-one-home) ·
[2. Audit of stpa-workbench v0](#2-redundancy-audit-stpa-workbench-artifact-schema-v0) ·
[3. Audit of our documents](#3-redundancy-audit-our-own-documents) ·
[4. OSCAL](#4-nist-oscal) · [5. The normalized model](#5-the-normalized-model) ·
[6. The migration](#6-the-migration) · [7. How to use it](#7-how-to-use-it) · [8. Owner decisions](#8-owner-decisions-2026-09-29)

## 1. The criterion: one fact, one home

1. Every fact has exactly one home: one field of one record.
2. A relation is stored on one side only (the dependent record points at what it depends on); its
   inverse is computed.
3. Anything derivable is computed, never stored: controller of a UCA, hazards of a scenario, whether
   a component is a controller or a controlled process, whether a constraint is enforced, counts.
4. Citation labels (`H-2`, `UCA-4`, `R-S3`, `CAST-50`) are the only human alias of a record. Documents
   cite labels; no document restates a label's title except a generated view.
5. Any redundancy that remains is a cache that a check proves consistent, and it says why it exists:
   the generated files (checked by a Go test in CI), the file name (= the id), the v0 export (never
   committed, regenerated on demand).

## 2. Redundancy audit: stpa-workbench artifact schema v0

Read: `docs/spec/artifact-schema-v0.md`, `schemas/v0/*.json`, `fixtures/deployment-recovery/`,
`tools/stpa_workbench_tools/` (validate, records, diff, impact) and RFC 0001 on the RFC branch
(`claude/stpa-workbench-rfc-1zzv3b`).

| # | Fact | Where v0 stores it | Authoritative copy | Can they disagree? | Fix (in §5's model) |
| --- | --- | --- | --- | --- | --- |
| W1 | A record's kind | `kind:`, the id prefix (`uca-…`), the folder, the schema file chosen | the id prefix (validator: `id-kind-mismatch`) | yes; the folder is never checked, and §2 says paths carry no meaning | drop `kind:`; the id prefix is the kind; folders are free (we use one flat `records/`) |
| W2 | A record's title | `title:` and the body's `# heading` (heading rule) | `title:` | yes, until the heading rule runs | drop the heading; a renderer adds it (the v0 export does) |
| W3 | The schema version | `schema:` in every record and in `project.yaml` | none stated | yes (mixed versions in one project) | project manifest only |
| W4 | A UCA's controller | `uca.controller` and its `action.from` | `action.from` (rule 5 only checks) | yes | derived; not a field |
| W5 | A UCCA contribution's controller | `contributions[].controller` and the action's `from` | the action | yes (rule 5) | derived |
| W6 | A component's identity | one `controller` record and one `process` record for the same real thing (A02), "said so in both bodies" | neither | yes: two titles, two descriptions, and edges attached to either | one `component`; its roles are computed (a controller iff it issues an action); the v0 export splits it |
| W7 | Claim ↔ evidence | `claim.evidence_records` and `evidence.supports`/`challenges` | both (validator checks the back-pointer) | yes | on the evidence only; the claim's list is computed |
| W8 | Assumption ↔ finding | `assumption.qualifies` and `uca`/`ucca`.`assumptions` | both; **nothing checks them against each other** | yes, silently | one side (`qualifies`, which the impact traversal already reads) |
| W9 | What a proposal changed | `proposal.changes` and the semantic diff base→head | the diff (rule 10) | yes | not stored: the proposal pins base and head, the diff is computed; the declared intent stays prose |
| W10 | A claim's assessment | the claim's `evidence` status and its evidence records' `result`s | unstated | yes | computed from the results; a reviewer's override is its own record with a rationale |
| W11 | Whether a constraint is implemented | `constraint.implementation` and the `implementation` of the mechanisms that implement it | unstated | yes | computed from the mechanisms (we render "Nothing yet (new)" when there are none) |
| W12 | A scenario's hazards | `scenario.hazards` and the hazards of its `findings` | unstated | yes | computed from the findings; stored only for a scenario without findings |
| W13 | Freshness of a belief | `feedback.freshness` and `process_model[].freshness_assumption` naming that feedback | unstated | yes, for the same feedback | one home (on the belief, or on the feedback, not both) |
| W14 | Which variables an action changes | `action.affects` (free strings) and the process's `state_variables` | the process | yes, unchecked (a typo creates a variable) | check `affects ⊆ state_variables` |
| W15 | A baseline's commit | `baseline.subject_commit` and its `decision.subject_commit` | the decision | yes | derived from the decision |
| W16 | A disposition's subject | `subject` and `path[-1]` (rule 11) | either | yes | `subject` derived from the path |
| W17 | That a baseline is superseded | `state: superseded` on it and `supersedes` on the newer one | the newer one's link | yes | derived |
| W18 | What a generated view derives from | `views[].derived_from` as folder paths | the records' kinds | yes, after a move (and it contradicts "no meaning from paths") | derived from kinds |
| W19 | A hazard's statement | `title`, `system_condition`, the body's first paragraph | none | yes (three phrasings) | one statement (`title`); prose only for rationale |
| W20 | The configuration a claim is about | `claim.target_configuration` and each `evidence.target_configuration` | the claim | yes | the evidence states only a deviation |
| W21 | The set of kinds and their reference targets | the JSON Schemas, and again as constants `ANY_KIND` and `REFS` in `validate.py` | the constants win (a new schema file is ignored) | yes: an extension kind validates in JSON Schema and is still rejected as `unknown-kind` | read kinds and ref targets from the schemas (e.g. an `x-ref-kinds` keyword) |
| W22 | The generated Mermaid's node map and edge ids (§8) | the `.mmd` file | the records | yes | a justified cache: generated, and the validator compares it (keep) |
| W23 | `label` vs `id` | two identifiers | `id` | no, if the label is the only alias | keep; documents must never restate label → title outside generated views (ours do: §3) |

## 3. Redundancy audit: our own documents

| # | Duplicated fact | Where it is today | Disagree now? | Its one home | After migration |
| --- | --- | --- | --- | --- | --- |
| O1 | Extension hazards H-E\*, H-G\*, H-L\* | STPA.md summary tables and research §1.2 tables | **yes**: STPA.md shortens the wording (all four piloted H-E rows differ, §6.3) and drops the losses column of H-E/H-G | `hazard` record | both tables generated (piloted for H-E1..3, H-E9) |
| O2 | Extension losses L-7, L-E1, L-G1 | STPA.md prose, research §1.1 tables | yes (L-E1 shortened in STPA.md); **L-G1 is in no table** | `loss` record | generated |
| O3 | Core losses restated per extension | langfuse §1.1 ("An incident is missed or prolonged", shortened), entra §1.1 ("here, …") | yes (wording) | the loss once; the per-extension instance is a separate statement in the extension's prose, not a table re-titling the loss | generated table of the loss plus hand prose |
| O4 | Extension requirements R-L\*, R-E\*, R-G\* | STPA.md one-line summaries and prose lists; research §1.8 full text | yes (summaries) | `requirement` record, one text | generated in both places |
| O5 | Hazard refinement (⊂) | STPA.md "⊂" column; research tables; langfuse prose "H-L1 ⊂ H-6; …" | possible | `hazard.refines` | generated column |
| O6 | Teaming requirement text | TM "Requirement" column and R-S\* (TM-4's vs R-S4's wording; TM-1 vs R-S1) | yes (two wordings of one obligation) | the requirement; `R.from` already links TM-4 → R-S4 | TM column generated from the inverse link (pilot keeps the text, flagged) |
| O7 | STPA-Sec mitigation text | SEC "Mitigation" and R-S7/R-S8/R-S9 | yes (wording) | the requirement (`R-S7.from = SEC-1..3`) | as O6 |
| O8 | Loss-scenario status | LS "Status" ("Open: needs SC-2 and SC-5", "Fixed: …") and R-S\*.From, CAST rows | possible | the links in `R.from`; the status computed from the requirements' and mechanisms' implementation | pilot keeps `resolution` as text, flagged |
| O9 | A UCA's controller | UCA "Controller" column and the control structure's edges | no (checked by generation) | `action.from` | derived (piloted) |
| O10 | The control actions | the UCA table's "Control action" column (a finer grain), the drawn edge labels, and the component descriptions ("ack, silence, escalate") | **yes**: UCA-10 "Return a result" and UCA-12 "Page on-call" are drawn as **feedback** in diagram B ("complete-through", "pages"); UCA-14 "Silence an alert" is drawn only as "acks" | one `action` record per control action; the drawn label is the group of its members' short labels | piloted (UCA-4, 5, 6, 8, 11, 14); UCA-10/12 need a modelling decision (§8) |
| O11 | Constraint enforcement | "Enforced today by" column | n/a | `mechanism` records implementing the constraint | generated (piloted) |
| O12 | Component descriptions | STPA.md diagram A, STPA.md diagram B, PRD A, PRD B | **yes**: Platform operators has 4 wordings; Central ClickHouse "tables, rollups, indexes" vs PRD B "hot data, indexes"; Lake snapshots "Iceberg (planned)" vs "snapshots, term index"; Entity catalog "tables and dictionaries" vs "entities, dictionaries"; On-call engineers 2 wordings | `component.description` | every diagram generated (piloted: the pilot kept diagram A's wording) |
| O13 | Edge labels | STPA.md "CAS leases, checkpoints" vs PRD A "CAS leases"; "rules, acks" | yes | `action.label` / `feedback.label` | generated |
| O14 | The control structure itself | STPA.md A and B (Mermaid), PRD A and B (hand-drawn SVG), research §1.3 (langfuse redraws A plus three controllers) | yes (O12, O13) | component, action and feedback records; a view is a placement | generated SVG, PRD widget and Mermaid (piloted for A and B) |
| O15 | CAST rows | STPA.md and the PRD | not checked here | `incident` record | both generated |
| O16 | Hazard citations in CAST | "Hazard" cells cite **H-8 (rows 29, 35, 39, 47), which no table defines** | — | the cell's prose; label mentions are derived links, checked | the check warns at authoring time (row 29 is in the pilot) |
| O17 | Hazards and UCAs in the verification plan | VERIFICATION.md §3 headings ("H-1 Acknowledged telemetry in no store (L-2, L-1)") and UCA rows restate titles and links, shortened | yes (wording) | the records | VERIFICATION.md cites labels only, or gets generated headings |
| O18 | Owner decisions | DECISIONS.md D36/D37/D38 and research §0/§11 "Owner decisions" lists | possible | DECISIONS.md | research notes cite the D-number (not STPA data; out of this model's scope) |
| O19 | Counts in prose | "Six losses …; seven hazards", "Ten requirements …" | will, on the next addition | computed | dropped or generated |
| O20 | CAST themes | "Systemic factors" issue lists, research §1.9 "how each CAST theme is avoided" tables | possible | a `themes` link on each incident (proposed) | generated lists |
| O21 | The citation catalogue | the traceability check parses STPA.md's tables | — | the records | `generated/labels.json` is the catalogue to read |
| O22 | Requirement priority | "all are P0 unless marked" plus a "(P1)" suffix in the text | — | `requirement.priority` | rendered as the suffix (piloted) |

## 4. NIST OSCAL

Read 2026-09-29: OSCAL **v1.2.3** (latest tag of `usnistgov/OSCAL`, tag commit 2026-08-06), its
metaschema sources (`src/metaschema/oscal_*_metaschema.xml` at that tag); the tool releases
`usnistgov/oscal-cli` **v1.0.3** and `oscal-compass/compliance-trestle` **v5.1.0** (latest tags).
Models: catalog, profile, component definition, system security plan (SSP), assessment plan,
assessment results, plan of action and milestones (POA&M), and the control mapping collection
(`mapping-collection`, relationships `equal-to`, `equivalent-to`, `subset-of`, `superset-of`,
`intersects-with`, `no-relationship`). Extension points: `prop` (name, value, `ns` URI, class) and
`link` (href, `rel`). Sources: <https://pages.nist.gov/OSCAL/>, <https://github.com/usnistgov/OSCAL/tree/v1.2.3/src/metaschema>.

| Our fact | OSCAL home | Native semantics? |
| --- | --- | --- |
| Requirements R-\*, system constraints SC-\* | catalog controls (groups per family; statement parts) | yes |
| Tailoring per deployment or environment | profile | yes |
| Mechanisms, and which component implements which requirement | component definition, `implemented-requirement` by `control-id` | yes |
| Test and traceability runs | assessment results: observations with `relevant-evidence`, findings targeting a control statement | yes |
| Known gaps with owner and date | POA&M items | yes |
| STPA-Sec requirements ↔ NIST SP 800-53 (AC-3, AC-6, AU-2, SI-10 …) | mapping collection | yes, and useful to an auditor |
| Losses, hazards, controllers, control actions, feedback, process models, UCAs, loss scenarios, STPA-Teaming, CAST | nothing; only `props`/`links` in our namespace | **no**: OSCAL tools cannot check these links, so our own checker would still be needed |

Against the criterion, OSCAL as the *source* adds redundancy rather than removing it: every object
has a UUID **and** (for controls) a human `id`; an SSP's by-component statements restate what a
component definition says; a POA&M carries its own `observation`, `risk` and `finding` copies
alongside the assessment results it follows from; every `prop` repeats its namespace URI. It is also
verbose to diff and to write by hand (trestle's Markdown authoring helps for catalogs, not for STPA).

**Verdict: OSCAL as a generated export, not the source (owner, 2026-09-29: not now; a later export).** The STPA core stays in the records of §5;
the assurance half is exported as OSCAL documents from them — a catalog (R-\*, SC-\*), component
definitions (mechanisms by component), assessment results (from the traceability runs), a POA&M (from
the known-gaps list), and a mapping collection (STPA-Sec requirements → 800-53) — and validated in CI
with oscal-cli (needs a JRE) or trestle (Python). A deeper adoption would only pay if an external
assessor consumes OSCAL; then the export is what they get. Not built in the pilot.

## 5. The normalized model

An adaptation of stpa-workbench v0, not a fork: one record per Markdown file, YAML front matter,
`<kind>-<6 hex>` identity, `label` as the citation alias, generated views. What changes is only the
redundancy of §2:

- no `kind:` (the id prefix is the kind), no per-record `schema:` (the manifest's), no heading
  (the title is in the front matter), file name = id (checked), one flat `records/` directory;
- the hex part is unique across kinds, so a record can change kind without a collision: the pilot's
  `component-8020ed` is `controller-8020ed` now, and `labels.json` still resolves the old id;
- links exist only in the control structure (below); a controller holds its own internals;
- links between records on one side; inverses and derived values computed (table below);
- prose fields that cite labels (CAST cells, requirement text) are links by scanning; a label that
  resolves to nothing is an **error** (it was a warning during the pilot).

| Kind | Fields (all have `id`, `state`) | Derived, never stored |
| --- | --- | --- |
| `loss` | `label`, `title` | the hazards leading to it |
| `hazard` | `label`, `title` (the system condition, with its worst-case environment), `losses`, `refines` (⊂) | its constraints, UCAs, refinements |
| `constraint` | `label`, `title`, `hazards` | "Enforced today by" (its mechanisms) |
| `mechanism` | `title`, `implements` (constraints, requirements, loss scenarios), `effect` (fixed, guarded, measured: for a scenario) | — |
| `controller` | `label`, `title`, `description`, `component_type`, `process_model`, `control_algorithm` | its links (the structure); whether it is also controlled |
| `controlled_process` | `label`, `title`, `description`, `owner` | its links |
| `uca` | `label`, `action` (a structure path), `category`, `context`, `hazards`, `variables` | controller (the path's upper end), the generated title |
| `scenario` (LS) | `label`, `title`, `findings` (UCAs) and/or `feedback` (structure paths), `factor`, `hazards` (only without findings), `variables`, `formerly` | hazards (its findings'), status (its mechanisms and the requirements that cite it) |
| `sec` (STPA-Sec) | `label`, `title` (adversary action), `unsafe`, `findings`, `hazards`, `mitigation` (only if no requirement cites it) | the "(UCA-8)" suffix; the requirements answering it |
| `teaming` | `label`, `title`, `text`, `hazards`, `requirement` (only if no requirement cites it) | the requirements answering it |
| `requirement` | `label`, `title`, `priority`, `from` (record ids, or labels of hand-kept tables) | the "(P1)" suffix; its mechanisms |
| `incident` (CAST) | `label`, `batch`, `title`, `found_by`, `hazard`, `controller`, `why`, `fix`, `lesson`, `variables` | hazards cited (scanned from `hazard`) |

### 5.1 The control structure: one document where duplicates cannot be written

[structure.yaml](structure.yaml) is the only place links exist. Its nodes are references to controller
and controlled-process records; its links are one map entry per ordered pair, keyed
`"<upper> -> <lower>"`, holding `control:` (runs down the arrow) and `feedback:` (runs back up) maps of
labelled entries. Diagrams A and B are **views**: a subset of nodes and their placement
([views/](views/)); every link between two placed nodes is drawn, so two views cannot disagree about a
shared link. Records cite an entry by its path, `<upper>-><lower>/<control|feedback>/<key>`.

What the shape rules out, and how:

| Form | How it is ruled out |
| --- | --- |
| a pair defined twice; an action or a feedback defined twice on one pair; a node named twice | **cannot be written**: the parser refuses any duplicate mapping key |
| a pair spelled another way (`a->b`, two spaces) | **cannot be written**: the key must be exactly `<name> -> <name>` |
| a controlled process at the upper end | **cannot be written**: a node's kind is its record id's kind (`controller-…` or `controlled_process-…`), and the upper end must be a controller |
| a link to a name that is not a node; an entry without a label; a link with no entries; an unknown field | **cannot be written** (parse errors with the line) |
| control in the wrong direction | **cannot be written**: direction is not a field |
| the same pair keyed in both orders; a self link; a cycle of control | **checked** (`stpa check` error) |
| a node naming no record; two nodes for one record; a controller or process record in no node | **checked** |
| a controller that controls nothing (it should be a controlled process) | **checked** |
| a record citing a path that does not exist, or of the wrong kind (a UCA on a feedback) | **checked** |
| a feedback entry no process-model variable of its upper controller cites; a control entry no rule of it issues; a controller citing a link it is not the upper end of | **checked** (controller internals) |

A controller can itself be controlled (on-call engineers → Telemetry UI → query service): it stays one
controller record; the v0 export computes v0's separate controller and process records (A02).

One page of it, for diagram A's consumer and ClickHouse: the controller record
(`records/controller-8020ed.md`, abridged)

```yaml
id: controller-8020ed
label: Consumer worker
title: Consumer workers
description: leases, time-bound inserts, count check and repair
component_type: software
process_model:
- name: statement
  meaning: "Each statement's outcome: committed, not applied, or unresolved until it can no longer land"
  updated_by: [consumer->clickhouse/feedback/answers]
- name: row_counts
  meaning: How many rows of each object central holds, by content key (D11)
  updated_by: [consumer->clickhouse/feedback/counts]
- name: fence
  meaning: "The deadline a statement carries, evaluated on ClickHouse's clock: sent + ttl - margin - budget (D9)"
  source: computed from the lease write's send time, the TTL and the margin
control_algorithm:
- name: insert a batch
  when: the lease is held, now + budget <= safe_until, objects are pending and no statement of the lane is unresolved
  uses: [lease, fence, pending_objects, statement]
  issues: [consumer->clickhouse/control/insert]
- name: repair a short count
  when: a settled statement's objects are short in central's counts
  uses: [statement, row_counts]
  issues: [consumer->clickhouse/control/repair]
state: accepted
```

the controlled-process record (`records/controlled_process-ed00c7.md`)

```yaml
id: controlled_process-ed00c7
title: Central ClickHouse
description: tables, rollups, indexes
state: accepted
```

and the structure entry that links them (`structure.yaml`)

```yaml
nodes:
  consumer: controller-8020ed         # Consumer workers
  clickhouse: controlled_process-ed00c7 # Central ClickHouse
links:
  consumer -> clickhouse:
    control:
      insert: {label: insert, title: Insert a statement, was: action-a2cc85}
      repair: {label: repair, title: Repair missing rows, was: action-50af26}
    feedback:
      answers: {label: answers}
      counts: {label: counts, was: feedback-80b0e6}
```

`title` is the full name the tables show ("Insert a statement" in the UCA table); `was` keeps the pilot's
action and feedback record ids resolvable (`generated/labels.json`).

### 5.2 Inside a controller: process model and control algorithm

A **process-model variable** has a `name`, its `meaning`, and where it comes from: `updated_by`, the
feedback entries (of this controller's own links) that update it, or `source`, a sentence for a belief no
feedback updates (the edge's own clock, a configuration, the Kubernetes API outside the structure). A
**rule** of the control algorithm has a `name` (what it decides: "take or renew the lease", "advance
the checkpoint"; unique within its controller, since it heads the rule in the detail diagram, where two
rules issuing the same action would otherwise read alike), a condition (`when`), the variables it `uses`,
and the control entries it `issues`. They were derived from what STPA.md (UCA contexts, loss scenarios, the CAST "flawed
process model" cells) and DECISIONS.md (D8, D9, D11, D12, D19, D21–D23, D29, D30, D35, D38) already say;
42 variables and 25 rules for the 10 controllers.

Checks: every feedback entry updates some variable of its upper controller; every variable has a source;
every control entry is issued by some rule; every rule has a name, unique within its controller; a rule uses only its controller's variables and issues only its
controller's links. UCAs name the variables their context is about (`variables: [lease, fence]`, resolved
against the UCA's controller); loss scenarios and CAST rows name them qualified (`consumer/lease`); a
flawed-process-model scenario or CAST row that names none is a **warning** (34, all CAST rows whose
controller is not in the structure: an upstream encoder, a fork patch, the development process).

Writing them down changed the structure where a belief had no feedback to come from: five feedback
entries were added (the edge's PUT outcome, the consumer's statement answers, GC's checkpoints and marks,
the sealer's snapshots, the query service's entities), and the telemetry producers became a controlled
process (the edge's acks and 503s act on them: UCA-1, UCA-3).

**Drawn** (option (b) of the prototypes; the others are in the D39 record): the overview (diagrams A and B)
keeps its boxes and gives each controller a strip with its two compartments, control algorithm (left, where
control leaves) and process model (right, where feedback arrives), with their sizes; one **detail
diagram per controller** ([generated/controller-*.svg](generated/)) draws the controller as the STPA
Handbook does: rules on the left (headed by the rule's name, the actions it issues on the line below), each starting the control arrows it issues; variables on the right, each
the end of the feedback arrows that update it; the controllers above and the nodes below as in the
structure. The order of rules, variables and nodes is the one with the fewest crossings (tried
exhaustively, deterministic). A Mermaid version of each detail is generated too; it is legible for small
controllers only, so STPA.md embeds the SVGs.

![The consumer's detail diagram](generated/controller-consumer.svg)

### What does not fit v0, and the decision for each

| Item | Decision |
| --- | --- |
| Hazard ⊂ hazard (H-E1 ⊂ H-3, H-6) | propose a `refines` field (overlay) |
| System-level constraint SC-\* derived from hazards | propose `constraint.hazards` (v0's `addresses` takes only scenarios and UCAs) |
| Loss without stakeholder records | propose `stakeholders` optional (overlay) |
| Requirements R-\* | propose a `requirement` kind: derives from any analysis record, has a priority |
| CAST rows | propose an `incident` kind (v0: "incidents … not in v0"): the eight CAST fields |
| A mechanism that settles a loss scenario, or implements a requirement | overlay: `mechanism.implements` takes constraints, requirements and scenarios |
| STPA-Sec rows | use v0 `scenario` as is, with `factors.control`, and `ext: {stpa-sec:mitigation}` (the answering requirements, or the row's own text) |
| STPA-Teaming rows | use v0 `scenario` with `factors.human` and `ext: {stpa-teaming:requirement}` |
| Loss-scenario status, factor, feedback flaw, former labels | `ext: {otel-chdb:…}` in the export (the status is derived) |
| Control actions and feedback | v0 `action` / `feedback` records, generated from the structure entries (the `was` id, else a hash of the path) |
| A process-model variable without feedback; the control algorithm | v0's `process_model[]` needs a `source_feedback`; the rest goes to `ext` |
| A controller's responsibility | one v0 `responsibility` per controller, from its description, scoped to what it controls |
| Diagram layout hints | not expressible (`views[]` is closed); kept in our `views/*.yaml` |

The overlay schemas are in [workbench-v0-overlay/](workbench-v0-overlay/); the notes for the
workbench RFC in [workbench-feedback.md](workbench-feedback.md).

## 6. The migration

### 6.1 What is migrated

| Kind | Records | Which |
| --- | --- | --- |
| loss | 9 | L-1..L-7, L-E1, L-G1 (L-G1 was in no table) |
| hazard | 33 | H-1..H-8 (H-8 new), H-L1..L8, H-E1..E9, H-G1..G8 |
| constraint | 8 | SC-1..SC-8 (SC-8 new, for H-8) |
| mechanism | 20 | the constraints' "Enforced today by", the fixes that settle LS-1..5 and LS-9, what implements R-S1, R-S3, R-S7..9, R-E8, R-G1..4 |
| controller | 10 | every controller of diagrams A and B, with process model and control algorithm |
| controlled_process | 5 | the four stores and the telemetry producers (new) |
| uca | 14 | UCA-1..16 but UCA-10 and UCA-12 (now feedback flaws, §8 item 3) |
| scenario | 10 | LS-1..LS-10 |
| sec | 8 | SEC-1..SEC-8 |
| teaming | 8 | TM-1..TM-8 |
| requirement | 40 | R-S1..S10, R-L1..L12, R-E1..E9, R-G1..G9 |
| incident | 57 | CAST rows 1–57, in their 21 tables (batches) |
| structure | 1 file | 15 nodes, 21 links, 28 control and 15 feedback entries (31 replace the pilot's 21 action and 10 feedback records) |

Labels still defined only in hand-kept tables (the extensions' UCA-L/E/G, LS-L/E/G, SEC-L/E/G and TM-L/E/G
rows, which no other document copies, and decision headings) are declared in `project.yaml`
(`hand_kept`); a requirement's `from` may cite them until they are records.

### 6.2 Outputs (`generated/`, all checked in CI)

| File | What |
| --- | --- |
| [control-structure-a.svg](generated/control-structure-a.svg), [control-structure-b.svg](generated/control-structure-b.svg) | the PRD style: levels top-down, bold name and quiet lines, stores tinted, people accented, control solid, feedback dashed, control left of feedback in a pair, loops around the sides, the compartment strip, a key; light and dark from the reader's colour scheme |
| `controller-<node>.svg`, `controller-<node>.mmd` | one detail diagram per controller (§5.2) |
| `control-structure-{a,b}.prd.jsx` | the overview as the PRD's widget code (JSX in SVG, the document's `--cds-*` tokens, a `data-claude-text-id` per text, keyed by record id or structure path) |
| `control-structure-{a,b}.mmd` | Mermaid in STPA.md's convention (ELK, edges written downward, the `fb` class) |
| `stpa-tables.md` | every table, between markers; the same sections are spliced into STPA.md and the research notes |
| `labels.json` | label → record; former labels; structure paths; the ids that moved (the citation catalogue) |

![Control structure A, generated](generated/control-structure-a.svg)

### 6.3 Check against the hand-kept documents before the splice (`stpa compare`)

Every migrated row was identical to its hand-kept row except where the owner decided a difference (§8):
L-7, L-E1, L-G1 now in the losses table; H-8 new; the loss-scenario status derived (LS-5..LS-10); the
STPA-Sec mitigation of SEC-1..5, 7 and the teaming requirement of TM-1..4, 6, 7 replaced by the requirement
that answers them; LS-6's and LS-8's UCA column now names the feedback flaw; R-S2 derives from LS-6 (was
UCA-10); CAST rows 7, 8, 35, 47's hazard; R-E7's text (the owner's revision, D37); `CAST 36` written as
`CAST-36` in the extension requirements' "From"; and the extensions' hazards and requirements with the
research notes' wording in STPA.md (they were summaries). The diagrams differ from the hand-drawn ones where
the copies disagreed (O12, O13), where UCAs name an action the drawing bundled (B's on-call → alerting reads
"acks, rules, silence"; operators → query service "access, routing"), and by the edges §5.2 added.

### 6.4 Validation with the workbench's own validator

`stpa export-v0` writes the records and the structure as a strict artifact-v0 project (with the overlay
kinds and fields), recomputing every copy v0 stores. `stpawb validate` (stpa-workbench tools, through
[stpawb_overlay.py](workbench-v0-overlay/stpawb_overlay.py)): **280 records, 0 diagnostics, 0 findings**
(the pilot's 10 "controller without any responsibility" findings are gone: each controller exports a
responsibility).

## 7. How to use it

```sh
cd otel-chdb/stpa/tools
go run ./cmd/stpa check          # schema, references, labels, structure, internals; fails if a generated file or section is stale
go run ./cmd/stpa render         # rewrite generated/ and the marked sections of STPA.md and the research notes
go run ./cmd/stpa compare        # generated tables vs hand-kept tables (useful when migrating one)
go run ./cmd/stpa export-v0 DIR  # the records as a stpa-workbench v0 project
go test ./...                    # golden tests (testdata/mini), the structure's refused and checked forms, "generated/ is up to date"
```

**Add a CAST row** (or any row): write a record, not a table row. Pick an id: the kind, a hyphen, six
random hex digits (`openssl rand -hex 3`); the check rejects a hex already used by any kind. For a CAST
row, `records/incident-<hex>.md` with `label: CAST-58`, the `batch` of its table (the `cast-*` names in
[views/stpa-tables.yaml](views/stpa-tables.yaml); a new batch is a new table there and a new pair of markers
under a heading in STPA.md), the eight fields, and `variables` if the flawed process model is one of a
controller in the structure. Then `render`, and commit the record with its regenerated outputs. Editing the
row in STPA.md instead fails CI (`TestRepositoryGeneratedUpToDate`).

```yaml
---
id: uca-057c96
label: UCA-6
action: gc->s3/control/delete          # the structure entry; GC is the controller, derived
category: too-early-too-late-wrong-order # not-provided | provided | too-early-too-late-wrong-order | stopped-too-soon-applied-too-long
context: Before every reader (consumer groups, sealer) has passed it
hazards: [hazard-6fa4d1]               # H-1
variables: [positions]                 # GC's process-model variable the context is about
state: accepted
---
```

Refer to records by id and to links by path, never by label; cite labels in prose.

**Change the structure.** Add or change a link in [structure.yaml](structure.yaml); the check then asks
for the controller side: a feedback entry needs a process-model variable that it updates, a control entry a
rule that issues it. Placement is in `views/control-structure-*.yaml` (rows, `span`, `col`, `width`,
`detail: false`, `internals`); every link between two placed nodes is drawn.

**The PRD.** Paste `generated/<view>.prd.jsx` (the overviews) and `generated/controller-<node>.prd.jsx`
(the detail diagrams, under "Controller internals") into the PRD's widgets; its text ids are stable across
re-renders as long as the records keep their ids and the structure its node names and entry keys.

## 8. Owner decisions (2026-09-29)

"Go with your recs, migrate the rest." How each was carried out:

1. **Adopt and migrate.** Done (§6): every table of STPA.md, the extensions' losses, hazards and
   requirements, and the control structure; STPA.md's and the research notes' tables and diagrams are
   generated sections now. Unresolved label mentions are errors.
2. **Canonical wording.** H-E hazards: research/entra-ingress.md's text (and likewise H-L, H-G, R-L, R-E,
   R-G: the research notes' full text; R-E7 is the owner's revised text of D37). Component descriptions:
   diagram A's. Edge labels: the more complete ("CAS leases, checkpoints"). The research notes' §1 tables that
   duplicated STPA.md are the same generated sections; their restated core losses became a list of the
   extension's instances, citing the labels.
3. **UCA-10 and UCA-12 are feedback flaws.** In the structure the query service's result is feedback to
   the Telemetry UI (the UI requests, the service answers), and a page is feedback to on-call (on-call
   controls the alerting engine through rules, acks and silences). Neither is a control action: UCA-10 is
   now LS-6's flaw on `ui->qs/feedback/complete-through` (feedback wrong), UCA-12 is LS-8's on
   `oncall->alerting/feedback/pages` (feedback missing); both labels resolve (`formerly`, the former-labels
   table, `labels.json`), and R-S2 derives from LS-6.
4. **H-8.** Rows 29 and 39 (and 7, 8, written "H-1 (availability)") name one hazard none of H-1..H-7 covers:
   H-8, "The pipeline cannot accept, ingest or serve telemetry while an incident needs it: a lane stalls,
   writes are refused, or queries fail closed" (L-1), with SC-8. Rows 35 and 47 are false pages: H-4
   already covers them ("a page fires for a condition that does not hold"), so they cite H-4. L-G1 is in the
   losses table.
5. **Generated from links.** A loss scenario's status comes from the mechanisms that settle it and the
   requirements that cite it; the STPA-Sec and STPA-Teaming requirement columns name the requirements that
   answer the row, or keep the row's own text when none does (SEC-6, SEC-8, TM-5, TM-8): one wording per
   obligation, checked.
6. **OSCAL: not now**; a later generated export (§4).
