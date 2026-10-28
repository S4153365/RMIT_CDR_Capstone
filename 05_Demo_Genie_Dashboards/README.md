# `05_Demo_Genie_Dashboards/` — deliverable 3

The AI/BI showcase: a governed Genie space over the Gold layer, and the dashboards built from it. The
space and the dashboards live in the workspace, not in this repository, so what is here is the material
needed to recreate them and the rules that make them correct.

| File | What it is |
|---|---|
| [`genie_space.md`](genie_space.md) | Space settings, the curated table list, and the instruction text to paste — including the definition, data-shape and refusal rules |
| [`dashboard_spec.md`](dashboard_spec.md) | The nine tiles, each tied to the benchmark query that produces its measure, plus the captions each one must carry |

The third piece of this deliverable is already elsewhere:
[`06_Genie_Vocabulary/02_Genie_Validation.py`](../06_Genie_Vocabulary/02_Genie_Validation.py) runs the
Q1–Q14 benchmark and carries the scoring sheet. It is the instrument that scores whether the space
answers correctly; these two files describe what the space and the dashboards should be.

## The gap that remains

Nothing here is evidenced until it has been run. The space must be created in the workspace, the
benchmark run, the scoring sheet filled in, and the dashboards built **as a researcher identity** —
not as an admin, which would render columns a researcher never sees. [`HANDOVER.md`](../HANDOVER.md) §3
tracks that as outstanding work.

Two files here are also the answer to a question the brief asks directly: how the semantic layer is
communicated to the Genie agent. The instruction block in [`genie_space.md`](genie_space.md) is that
semantic layer, written out.
