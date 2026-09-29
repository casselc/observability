# STPA as records (pilot, proposed: DECISIONS.md D39)

The STPA of this repository ([STPA.md](../STPA.md) and the STPA sections of
[research/langfuse.md](../research/langfuse.md), [research/entra-ingress.md](../research/entra-ingress.md) and
[research/grants.md](../research/grants.md)) is kept today as hand-written Markdown tables and Mermaid, and
several facts live in two to four places that already disagree. This directory is a **pilot** of the
alternative: every STPA fact is one field of one record, and the tables, the control-structure
diagrams (SVG in the style of the PRD, the PRD's own widget code, Mermaid) and the label catalogue are
generated from the records.

**Status: pilot, paused for the owner's review of the design** (the owner asked for the audit and the
design before a full migration). 95 records are migrated (counts in §6); `STPA.md` is unchanged.

Contents: [1. Criterion](#1-the-criterion-one-fact-one-home) ·
[2. Audit of stpa-workbench v0](#2-redundancy-audit-stpa-workbench-artifact-schema-v0) ·
[3. Audit of our documents](#3-redundancy-audit-our-own-documents) ·
[4. OSCAL](#4-nist-oscal) · [5. The normalized model](#5-the-normalized-model) ·
[6. The pilot](#6-the-pilot) · [7. How to use it](#7-how-to-use-it) · [8. Owner decisions](#8-what-the-owner-must-decide)

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

**Verdict: OSCAL as a generated export, not the source.** The STPA core stays in the records of §5;
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
- the hex part is unique across kinds, so a record can change kind (e.g. `sec` → v0 `scenario` in
  the export) without a collision;
- one `component` per real thing; controller or controlled process is computed;
- links on one side; inverses and derived values computed (table below);
- prose fields that cite labels (CAST cells, requirement text) are links by scanning; an unknown
  label is reported (warning in the pilot, error after the full migration).

| Kind | Fields (all have `id`, `state`) | Derived, never stored |
| --- | --- | --- |
| `loss` | `label`, `title` | the hazards leading to it |
| `hazard` | `label`, `title` (the system condition), `losses`, `refines` (⊂) | its constraints, UCAs, refinements |
| `constraint` | `label`, `title`, `hazards` | "Enforced today by" (its mechanisms) |
| `mechanism` | `title`, `implements` | — |
| `component` | `title`, `label` (short name used in tables), `description`, `component_type` | controller / controlled process; its actions and feedback |
| `action` | `label` (short, drawn), `title` (full name), `from`, `to` | the drawn edge (all actions between two components) |
| `feedback` | `label`, `title`, `from` (the process side), `to` (a controller: checked) | — |
| `uca` | `label`, `action`, `category`, `context`, `hazards` | controller (`action.from`), the generated title |
| `scenario` (LS) | `label`, `title`, `findings`, `resolution` (text, flagged O8) | hazards (its findings') |
| `sec` (STPA-Sec) | `label`, `title` (adversary action), `unsafe`, `findings`, `hazards`, `mitigation` | the "(UCA-8)" suffix |
| `teaming` | `label`, `title`, `text`, `hazards`, `requirement` (text, flagged O6) | — |
| `requirement` | `label`, `title`, `priority`, `from` | the "(P1)" suffix; which TM/SEC/LS it answers (inverse) |
| `incident` (CAST) | `label`, `batch`, `title`, `found_by`, `hazard`, `controller`, `why`, `fix`, `lesson` | hazards cited (scanned from `hazard`) |

Views (`views/*.yaml`) hold presentation only: a control-structure view places components in rows and
columns and gives the diagram's title; a tables view names columns and which hand-kept document holds
the same table today. Every box, edge, label and cell comes from the records.

How the duplicates collapse: O1, O2, O4, O5, O9, O11, O12–O15, O19, O21, O22 become generated views of
one record each; O6, O7, O8, O20 become links (the remaining text fields are flagged for the full
migration); O16 is caught by the check; O3, O17, O18 are prose that should cite labels instead of
restating titles.

### What does not fit v0, and the decision for each

| Item | Decision |
| --- | --- |
| Hazard ⊂ hazard (H-E1 ⊂ H-3, H-6) | propose a `refines` field (overlay) |
| System-level constraint SC-\* derived from hazards | propose `constraint.hazards` (v0's `addresses` takes only scenarios and UCAs) |
| Loss without stakeholder records | propose `stakeholders` optional (overlay) |
| Requirements R-\* | propose a `requirement` kind: derives from any analysis record, has a priority |
| CAST rows | propose an `incident` kind (v0: "incidents … not in v0"): the eight CAST fields |
| STPA-Sec rows | use v0 `scenario` as is, with `factors.control`, and `ext: {stpa-sec:mitigation}` |
| STPA-Teaming rows | use v0 `scenario` with `factors.human` and `ext: {stpa-teaming:requirement}` |
| LS status | `ext: {otel-chdb:resolution}` in the export (to become derived, O8) |
| Diagram layout hints | not expressible (`views[]` is closed); kept in our `views/*.yaml` |

The overlay schemas are in [workbench-v0-overlay/](workbench-v0-overlay/); the notes for the
workbench RFC in [workbench-feedback.md](workbench-feedback.md).

## 6. The pilot

### 6.1 What is migrated

| Kind | Records | Which |
| --- | --- | --- |
| loss | 7 | L-1..L-6, L-E1 |
| hazard | 11 | H-1..H-7, H-E1, H-E2, H-E3, H-E9 (the extension, from research/entra-ingress.md §1.2) |
| constraint | 3 | SC-1, SC-2, SC-6 |
| mechanism | 2 | SC-1's and SC-6's "Enforced today by" |
| component | 14 | every box of control structures A and B (Platform operators, Central ClickHouse, Entity catalog and Lake snapshots once, not twice) |
| action | 21 | every drawn control action of A and B, split where a UCA names one member |
| feedback | 10 | every drawn feedback of A and B |
| uca | 6 | UCA-4, 5, 6, 8, 11, 14 |
| scenario | 4 | LS-2, 3, 4, 7 |
| sec | 6 | SEC-1, 2, 3, 5, 6, 7 |
| teaming | 3 | TM-2, 3, 4 |
| requirement | 3 | R-S4, R-S7, R-S9 |
| incident | 5 | CAST 1, 2, 29, 50, 52 (three batches) |

### 6.2 Outputs (`generated/`, all checked in CI)

| File | What |
| --- | --- |
| [control-structure-a.svg](generated/control-structure-a.svg), [control-structure-b.svg](generated/control-structure-b.svg) | the PRD style: levels top-down, bold name and quiet lines, stores tinted, people accented, control solid, feedback dashed, control left of feedback in a pair, a long loop around the right side, a key; light and dark from the reader's colour scheme |
| `control-structure-{a,b}.prd.jsx` | the same drawing as the PRD's widget code (JSX in SVG, the document's `--cds-*` tokens, a `data-claude-text-id` per text, keyed by record id) |
| `control-structure-{a,b}.mmd` | Mermaid in STPA.md's convention (ELK, edges written downward, the `fb` class) |
| `stpa-tables.md` | the tables, each between `<!-- stpa:begin NAME -->` / `<!-- stpa:end NAME -->` markers, ready to splice into STPA.md |
| `labels.json` | label → id, kind, title, path: the citation catalogue |

![Control structure A, generated](generated/control-structure-a.svg)

### 6.3 Check against today's documents (`stpa compare`)

Every piloted row of STPA.md's tables is **identical** to the hand-kept row (losses 6/6, hazards 7/7,
constraints 3/3, UCAs 6/6, loss scenarios 4/4, STPA-Sec 6/6, STPA-Teaming 3/3, requirements 3/3, CAST
5/5, research/entra-ingress.md §1.2 4/4), except the four H-E rows of STPA.md, whose wording is a
shortened second copy of the research note's (O1). The pilot keeps the research note's text and
reports the difference; it does not choose silently. The diagrams differ from the hand-drawn ones
where the copies disagreed (O12, O13) and in one deliberate case: B's on-call → alerting edge reads
"acks, rules, silence", since UCA-14 is on silencing.

### 6.4 Validation with the workbench's own validator

`stpa export-v0` writes the records as a strict artifact-v0 project (with the overlay kinds and
fields), recomputing every copy v0 stores (a UCA's controller, a scenario's hazards, the
controller/process split, the H1 heading). `stpawb validate` (stpa-workbench tools, in a scratch venv,
through [stpawb_overlay.py](workbench-v0-overlay/stpawb_overlay.py), which adds the overlay's kinds to
the validator's constants): **101 records, 0 diagnostics, 10 findings**, all "controller without any
responsibility" — a validator rule that is not in the spec's rule list, and our analysis has no
responsibility records (the component description plays that role).

## 7. How to use it

```sh
cd otel-chdb/stpa/tools
go run ./cmd/stpa check          # schema, references, labels, structure; fails if generated/ is stale
go run ./cmd/stpa render         # rewrite generated/ from the records
go run ./cmd/stpa compare        # generated tables vs the hand-kept tables in STPA.md and research/
go run ./cmd/stpa export-v0 DIR  # the records as a stpa-workbench v0 project
go test ./...                    # golden tests (testdata/mini) and "generated/ is up to date"
```

**Add a record.** Pick an id: the kind, a hyphen, six random hex digits (`openssl rand -hex 3`); the
check rejects a hex already used by any kind. Write `records/<id>.md`:

```yaml
---
id: uca-057c96          # UCA-6 as it is stored
label: UCA-6
action: action-e7ecbf   # "Delete a slot", from GC to the S3 lanes: GC is the controller, derived
category: too-early-too-late-wrong-order   # not-provided | provided | too-early-too-late-wrong-order | stopped-too-soon-applied-too-long
context: Before every reader (consumer groups, sealer) has passed it
hazards: [hazard-6fa4d1] # H-1
state: accepted
---
```

Refer to other records by id, never by label; cite labels in prose. Then `render`, and commit the
record with its regenerated outputs (CI's `TestRepositoryGeneratedUpToDate` fails otherwise).

**Change a diagram.** Placement is in `views/control-structure-*.yaml` (rows, `span`, `col`,
`detail: false`); everything else is a record. The layout is deterministic: fixed 3-column grid,
straight verticals where the corridor is clear, an L-route otherwise, a lane on the right for a loop
that would cross a box.

**The PRD.** Paste `generated/<view>.prd.jsx` into the PRD's widget; its text ids are stable across
re-renders as long as the records keep their ids.

## 8. What the owner must decide

1. Adopt the normalized profile (§5) as the source of STPA.md's tables and every control-structure
   diagram, and migrate the rest (the other 10 UCAs, 6 loss scenarios, 7 requirements, 50 CAST rows,
   the Langfuse, Entra and grants extensions), then replace STPA.md's tables with the generated ones
   between markers.
2. The canonical wording where copies disagree: the H-E summaries vs the research text (O1); the
   component descriptions (O12); "CAST leases, checkpoints" vs "CAST leases" (O13).
3. UCA-10 ("Return a result") and UCA-12 ("Page on-call"): control actions (then diagram B draws them
   solid) or feedback (then the UCAs become feedback flaws in loss scenarios).
4. H-8, cited by CAST 29, 35, 39, 47: define it (availability?) or correct the rows.
5. Whether TM/SEC "Requirement"/"Mitigation" columns and LS "Status" become generated from the
   requirements that cite them (O6–O8).
6. OSCAL as a generated export (§4): yes or not now.
