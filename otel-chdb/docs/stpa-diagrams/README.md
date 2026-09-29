# STPA diagrams in Mermaid: options compared

The owner asked (2026-09-29) for the STPA control structures to be Mermaid, drawn as close to the
STPA Handbook style as Mermaid allows: controllers stacked above the processes they control, control
actions down, feedback up (control on the left of a pair, feedback on the right), orthogonal edges,
plain rectangles, feedback dashed or grey. This page records what was tried on STPA.md's control
structure A and why the chosen form won. The rules themselves are in
[STPA.md, "Diagram conventions"](../../STPA.md).

## Where the diagrams must render (checked 2026-09-29)

| Renderer | Mermaid | ELK layout | How checked |
| --- | --- | --- | --- |
| GitHub Markdown (`viewscreen.githubusercontent.com`) | **11.17.2** | **not registered**: `layout: elk` logs "Layout algorithm elk is not registered. Using dagre as fallback" and draws with dagre | the production bundle `mermaidMarkdown-035ded29910819bc6e5e.js` (version string and the fallback in `getRegisteredLayoutAlgorithm`), and by rendering each diagram with that bundle: headless Chromium loaded the real viewscreen page (assets fetched with curl) under a stand-in `github.com` parent page that posts the diagram, as github.com does. Its `securityLevel` is `antiscript`; it also has a dark theme. An open request to enable ELK: [community discussion 138426](https://github.com/orgs/community/discussions/138426) (opened 2024-09-11, still open) |
| Mermaid 12.0.0 (released 2026-09-10): mermaid.live, `mmdc` 12, editors on 12 | 12.0.0 | **bundled and the default** for flowcharts ([layouts doc](https://mermaid.js.org/config/layouts.html), read 2026-09-29) | rendered with `@mermaid-js/mermaid-cli` 12.0.0 |
| claude.ai doc / PRD renderer | unknown | unknown | not checked. The chosen syntax works on 11.17 without ELK, so a renderer on 11.x draws what GitHub draws |

A block that sets `layout: elk` therefore renders everywhere: ELK where it is available and dagre
where it is not. Whatever makes it look like STPA has to work in dagre too.

## Options tried (same content, STPA.md view A)

| # | Form | Result | Image |
| --- | --- | --- | --- |
| 1 | Current: `flowchart TB`, dagre, feedback written `P -.-> C` | levels wrong: GC sits above the operators, and the consumer and aggregator below S3, because every feedback edge points down in the source and dagre ranks by edge direction | [GitHub](1-current-github.png) |
| 2 | `block-beta` grid, 3 rows | levels right, fixed grid, the same on both renderers; but straight diagonal edges, a control/feedback pair between two blocks is drawn on one line with the labels overprinted, no dashed feedback | [12](2-block-beta.png) |
| 3 | `swimlane-beta` (new in 11.16) with one lane per level | in `TB` the lanes are columns, not rows, so levels read left to right and the flow zig-zags down one tall column | [12](3-swimlane-beta.png) |
| 4 | Flowchart, ELK with model-order cycle breaking (`cycleBreakingStrategy: MODEL_ORDER`), feedback still `P -.-> C` | on Mermaid 12: correct levels, orthogonal edges, control left and feedback right. On GitHub (dagre) the same levels are wrong as in 1: the ELK settings are ignored there | [12](4-elk-feedback-as-written.png), [GitHub](4-elk-feedback-as-written-github.png) |
| 5 | **Chosen**: flowchart, `layout: elk`, every edge written downward; feedback `C fbN@<-.->|label| P` with `classDef fb stroke:#888,marker-end:none` | the source has no cycle, so ELK and dagre both put controllers above processes. The class removes the arrowhead at the process, leaving one head at the controller, so feedback reads upward; dashed grey. Control is left of feedback in each pair on both renderers. Orthogonal on 12, curved on GitHub | [12](5-chosen-elk.png), [GitHub](5-chosen-github.png), [GitHub dark](5-chosen-github-dark.png) |

Also considered: `architecture-beta` (icon-and-group service maps, edges attach to fixed sides of
a node, and edges take no labels in the 12.0.0 syntax, so no action or feedback names); `curve: step` for orthogonal
edges under dagre (on GitHub its segments share rows and run into the stores' sides, so labels
become ambiguous: rejected); invisible `~~~` links to force levels under dagre (they cannot fix a
process that feeds back to two controllers on the same level, because dagre only reverses an edge
whose target is an ancestor in its depth-first walk; they also skew ELK's layers); one subgraph per level
(tried with feedback written `P -.-> C`: ELK put the level boxes side by side and routed edges
around them; dagre let the boxes overlap and scatter; neither gives level rows).

Why 5: it is the only form whose levels are right on GitHub as well as on Mermaid 12, it keeps
edge labels and the dashed feedback, and it needs nothing beyond flowchart syntax that 11.17
already has (edge ids, edge classes). The cost is one line per feedback edge in the class list,
and a source that reads `<-.->` for feedback. If a renderer ever drops `marker-end:none` from a
class, feedback degrades to a dashed double-headed line, still distinguishable from control.

## Converted to this form

STPA control structures: STPA.md A and B; research/langfuse.md §1.3; research/entra-ingress.md §1.3
(its organisational edges, dashed before, are control actions and are now solid; the token and
status-code returns are feedback); research/grants.md §1.3 (was an ASCII drawing).
ASCII flow drawings (same Mermaid front matter; data flows solid and downward; an optional path
dashed and labelled as such): DECISIONS.md "The pipeline in one picture", deploy/README.md
"Topology", query/README.md (HyperDX adapter path), entities/rwproxy/README.md §2,
research/entra-ingress.md §3.
Left as text on purpose: key layouts and directory trees (research/lake-ui.md, model/S3NATIVE.md,
research/grants.md scope tuple, acceptance/RUNBOOK.md, entities/controller/README.md), result
tables and command transcripts, the chdb-go version annotation, and formulas.
