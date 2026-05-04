# gluten-coverage

Backend-agnostic coverage measurement for Apache Gluten integrations. Walks an executed Spark
plan and classifies each node as Native, Fallback, Adapter, or Vanilla against a YAML-declared
feature matrix. Each integration (Delta, Iceberg, Hudi, Paimon) registers its matrix via
`java.util.ServiceLoader`.

The tool gates regressions on every PR that touches an integration's code paths and produces
both a machine-readable JSON report (for CI gating) and a human-readable Markdown report (for
PR comments and release notes).

---

## Why this exists

Whether a given query runs natively under Gluten or falls back to Spark JVM is, today, something
a developer determines by reading the executed plan. There is no automated rollup, no per-PR
delta, no leadership-visible percentage. This tool changes that.

For each integration, a curated matrix of representative operations (declared as YAML) is
executed against a SparkSession with the integration enabled. The tool captures every executed
plan, classifies each node, and produces a coverage report. The report lands as a PR comment;
the JSON behind it gates the PR against a committed baseline so regressions cannot land
silently.

---

## What it produces

Two complementary metrics, both surfaced in every report:

- **Entry-pure-native percent (headline)** -- `entries_with_zero_fallback_nodes /
  total_actionable_entries`. User-faithful: an integration's operation either runs fully native
  or it doesn't from the user's perspective. Even one fallback node means hitting the JVM tax.
  The CI gate's regression check is on this metric.

- **Node-weighted percent (detail)** -- `native_nodes / (native + fallback + tax_adapters)`.
  Surfaces partial progress that the entry-pure-native metric cannot show. A PR that converts
  4 of 5 nodes in a MERGE plan to native shifts node-weighted upward but leaves
  entry-pure-native unchanged. Both signals are useful: entry-pure-native is the gate-on
  metric (a regression there means a user-visible regression); node-weighted is the diagnostic.

Both formulas are documented in the JSON output schema so external consumers do not have to
guess.

`benign_adapters` (adapter pairs entirely inside a single `WholeStageTransformer` boundary) and
`vanilla_nodes` (e.g., a `Subquery` broker) are excluded from both numerator and denominator
of the node-weighted formula. Vanilla nodes are not work that *should* run natively, so
penalising them inflates the failure rate spuriously.

---

## Architecture

```
                 +--------------+
       YAML  --> | MatrixLoader | --> matrices (one per integration)
                 +--------------+
                                +-----+
                       spark.sql| Run |
                       <------- |     | <-- MatrixEntry
                                +--|--+
                                   |
                                   v
                          QueryExecutionListener
                                   |
                                   v
                              Classifier --> PlanReport per entry
                                   |
                                   v
                            ReportBuilder --> CoverageReport
                                   |
                                   v
                       +------+   +-------+
                       | JSON |   |  MD   |
                       +------+   +-------+
                                   |
                                   v
                            BaselineDiffer (optional)
                                   |
                                   v
                               GateCheck
```

Pipeline:

1. `MatrixLoader` discovers all `MatrixDescriptor` implementations via
   `java.util.ServiceLoader`, loads each integration's YAML matrix, validates schema version
   and id uniqueness, drops archived entries.
2. The runner constructs a single SparkSession (with the integration's plugin/extensions
   registered) and registers a `CoverageQueryExecutionListener` on it.
3. For each matrix entry, the runner runs `setup_sql` (untagged), then tags the parsed
   logical plan of `query_sql` with the entry id (`TreeNodeTag`) before executing, then runs
   `teardown_sql` (untagged).
4. The listener fires `onSuccess` with the post-AQE `QueryExecution`. It reads the entry id
   from `qe.logical`, classifies `qe.executedPlan` via the Classifier, and appends to the
   collector.
5. `ReportBuilder` aggregates the collector into a `CoverageReport` with summary, per-feature
   breakdown, per-entry detail, and a discrepancy report against the upstream feature-claims
   doc.
6. `JsonEmitter` and `MarkdownEmitter` write the artefacts. `BaselineDiffer` (when a baseline
   is provided) computes the delta and produces a `GateCheck` that the CI workflow inspects.

---

## The classifier

Walks the executed plan via `TreeNode.foreach` (depth-first, root-first) and labels every
node with one of:

- **Native** -- node executes on Velox (or another Gluten backend).
- **Fallback** -- node executes on the JVM because Gluten declined or refused to offload.
- **Adapter** -- a `ColumnarToRowExec` / `RowToColumnarExec` boundary. Tax-adapter when the
  parent is not native (the round-trip is a real cost); benign-adapter otherwise.
- **Vanilla** -- a vanilla Spark plan node with no Gluten transformer counterpart (e.g., a
  Subquery broker). Reported but excluded from the headline.
- **Unknown** -- a node the classifier does not recognise. Forces a tool-side error rather
  than silently bucketing into Native.

Decision tree (in order):

1. `FallbackTags.nonEmpty(node)` -> **Fallback** (reason from the tag).
2. `node.isInstanceOf[WholeStageTransformer]` -> **Native** (wraps a native region).
3. `node.isInstanceOf[TransformSupport]` -> **Native** (individual transformer).
4. `node.isInstanceOf[ColumnarToRowExecBase]` -> **Adapter** (Columnar -> Row).
5. `node.isInstanceOf[RowToColumnarExecBase]` -> **Adapter** (Row -> Columnar).
6. `node.isInstanceOf[GlutenPlan]` -> **Native** (rare; non-TransformSupport Gluten node).
7. `CounterpartMap.hasCounterpart(opClass)` -> **Fallback** (untagged-fallback; defensive).
8. otherwise -> **Vanilla**.

The classifier reuses Gluten's own runtime signals (`FallbackTags`, `TransformSupport`,
`WholeStageTransformer`, `ColumnarToRowExecBase`, `RowToColumnarExecBase`, `GlutenPlan`).
This is deliberate: the tool's verdict matches Gluten's actual offload behaviour by
construction. Re-implementing classification logic would create drift.

The "untagged fallback" branch (step 7) catches plans where validator tagging skipped — a
vanilla node whose class appears in `CounterpartMap` (e.g., `FilterExec`, `ProjectExec`) is
classified as Fallback rather than Vanilla. The catalogue is conservative: missing entries
classify as Vanilla, which understates the fallback rate but never overstates it.

---

## Plan boundaries

For non-pure-native entries the report surfaces *where* in the plan the engine fell back:

- **`fallback_nodes` in the JSON** -- a deduplicated list of `(op_class, depth, reason)` for
  every fallback node in the entry's plan. Machine-readable.
- **`plan_tree` in the Markdown (verbose mode)** -- a depth-indented dump of the executed
  plan with verdict markers per node:

```
[V] WriteIntoDataSourceV2
  [A:tax] ColumnarToRowExec
    [N] WholeStageTransformer
      [N] HashAggregateExecTransformer
        [N] DeltaScanTransformer
  [F] MergeIntoCommandEdge -- "MERGE INTO not yet offloaded"
```

Markers: `[N]` Native, `[F]` Fallback, `[A:tax]`/`[A:benign]` Adapter, `[V]` Vanilla,
`[?]` Unknown.

---

## Running

The tool is a standalone JVM driver. With gluten-coverage and one or more integration jars
on the classpath:

```
spark-submit \
  --class org.apache.gluten.coverage.runner.CoverageRunner \
  gluten-coverage-<version>.jar \
  --matrix=delta \
  --output=target/coverage.json \
  --markdown=target/coverage.md
```

CLI options:

| Flag | Default | Purpose |
|---|---|---|
| `--matrix=ID` | (all) | Run only the matrix with the given id (e.g. `delta`). Without this, every matrix discovered via ServiceLoader runs. |
| `--output=PATH` | `target/coverage.json` | JSON output path. |
| `--markdown=PATH` | `target/coverage.md` | Markdown output path. |
| `--baseline=PATH` | (none) | Optional baseline JSON for diff and gate computation. |
| `--mode=full|smoke` | `full` | Run mode. (Smoke is reserved; not yet implemented.) |
| `--timeout=SECONDS` | `900` | Wall-clock budget for the entire run. |
| `--verbose` | off | Include all entries (and their plan trees) in the markdown. Otherwise only non-native entries are detailed. |
| `--delta-version=X` | (system property) | Override Delta version reported in the environment block. |
| `--gluten-version=X` | (system property) | Override Gluten version reported. |
| `--backend=X` | `velox` | Override backend reported. |

Exit codes: 0 = success, 2 = no matrices found, non-zero = tool-side failure.

---

## Authoring a matrix

Each integration ships a YAML at a stable resource path (e.g.
`coverage/delta-matrix.yaml` for Delta). Schema:

```yaml
schema_version: 1
matrix_id: <stable-id>          # e.g. "delta", "iceberg"
entries:
  - id: <stable-id>             # baseline-breaking change to rename; retire via archived: true
    feature: <bucket>           # high-level grouping; cross-walked to upstream feature-claims doc
    operation: <free-text>      # what the entry tests
    dtype: <type-or-na>
    expected: native | fallback | partial | unknown
    upstream_claim: yes | no | partial | not-tested
    delta_min_version: "3.2"        # optional version gate; entry skipped on older Delta builds
    spark_min_version: "3.5"        # optional version gate
    config:                         # optional per-entry Spark conf overrides; restored after
      "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite": "true"
    setup_sql: |                    # multi-statement; split on ;
      CREATE TABLE ...;
      INSERT INTO ...;
    query_sql: |                    # the SQL whose plan is captured; tagged with entry id
      SELECT ...;
    teardown_sql: |
      DROP TABLE ...;
    notes: "Optional human note"
```

The runner runs `setup_sql` (untagged, listener ignores), `query_sql` (tagged via TreeNodeTag
on `qe.logical`; listener captures the executed plan), then `teardown_sql` (untagged).

Field semantics:

- `id` -- stable, referenced by the diff machinery. Renaming is a baseline-breaking change;
  retire via `archived: true` and add a new entry instead.
- `expected` -- one of `native`, `fallback`, `partial`, `unknown`. Used by the report to
  highlight regressions: an entry expected `native` that classified `fallback` is flagged.
- `upstream_claim` -- copied from the integration's upstream feature-claims doc (for Delta:
  `apache/gluten/docs/get-started/VeloxDelta.md`). The discrepancy report surfaces drift
  between upstream documentation and measured behaviour.
- `delta_min_version` / `spark_min_version` -- entry is skipped (and reported as
  `verdict: skipped`) if the running build is below the minimum.
- `config` -- per-entry config overrides applied via `spark.conf.set` before the entry runs
  and restored after. State does not leak to the next entry.

---

## Registering a new matrix

1. Implement `org.apache.gluten.coverage.matrix.MatrixDescriptor` in your integration's
   `src/main/scala`:

   ```scala
   class MyMatrixDescriptor extends MatrixDescriptor {
     override def id: String = "myintegration"
     override def matrixResourcePath: String = "coverage/my-matrix.yaml"
     override def upstreamClaimsRows: Map[String, String] = Map(
       "my-feature-bucket" -> "Upstream doc row label"
     )
   }
   ```

2. Drop the YAML at the resource path returned by `matrixResourcePath`.
3. Register via `META-INF/services/org.apache.gluten.coverage.matrix.MatrixDescriptor` (one
   fully-qualified class name per line).
4. Add `gluten-coverage` as a `provided` dependency in your integration's pom.

The Delta integration in `gluten-delta/` is the worked example.

---

## Output format reference

### JSON

The schema is documented inline in the case classes under `org.apache.gluten.coverage.report`.
Top-level shape:

```json
{
  "schema_version": 1,
  "tool_version": "0.1.0",
  "generated_at": "2026-04-30T10:15:30Z",
  "environment": {
    "gluten_version": "1.6.0",
    "spark_version": "3.5.5",
    "delta_version": "3.3.2",
    "scala_binary_version": "2.12",
    "jdk_version": "17",
    "backend": "velox"
  },
  "matrix": {
    "id": "delta",
    "schema_version": 1,
    "entry_count": 175
  },
  "summary": {
    "entry_pure_native_percent": 53.0,
    "node_weighted_percent": 67.3,
    "entries_pure_native": 53,
    "entries_partial": 8,
    "entries_fallback": 47,
    "entries_metadata": 6,
    "entries_skipped": 12,
    "entries_errored": 0,
    "node_native": 213,
    "node_fallback": 92,
    "node_tax_adapters": 25,
    "node_benign_adapters": 24,
    "node_vanilla": 5,
    "node_unknown": 0
  },
  "by_feature": [
    {"feature": "cow-read", "entry_pure_native_percent": 88.0, "node_weighted_percent": 92.0, ...}
  ],
  "discrepancies": [
    {"id": "cdf-batch-read", "upstream_claim": "yes", "measured": "fallback", "delta": "regression-vs-doc"}
  ],
  "entries": [
    {
      "id": "cow-read-select-basic-int",
      "feature": "cow-read",
      "operation": "select",
      "dtype": "int",
      "verdict": "native",
      "expected": "native",
      "regression": false,
      "duration_ms": 412,
      "plan_summary": {"native": 4, "fallback": 0, "tax_adapters": 0, ...},
      "fallback_reasons": [],
      "fallback_nodes": [],
      "plan_tree": null
    }
  ],
  "gate_check": {
    "entry_pure_native_delta_percent": 0.0,
    "node_weighted_delta_percent": 0.0,
    "regressions": [],
    "tool_failures": []
  }
}
```

The `entries` array is sorted by `id` for byte-deterministic output. The `by_feature` array is
sorted alphabetically by feature.

### Markdown

Layout:

```
# <Integration> on Gluten - Coverage Report

**Generated:** ...
**Tool:** gluten-coverage <version>
**Environment:** Spark X + Integration Y + Gluten Z + Scala 2.12 + JDK 17 (velox)

## Headline

| Metric | Value |
|---|---:|
| Entry-pure-native (headline) | XX.X% (M of N) |
| Node-weighted (detail)        | XX.X%           |
| Entries: pure-native / partial / fallback | M / M / M |
...

## By feature
| Feature | Pure-native | Node-weighted | Entries (pure / fallback / total) |
...

## Gate check (when --baseline given)
- Entry-pure-native delta: +X.Y%
- Regressions (N): ...

## Upstream documentation discrepancies
| Entry | Upstream claim | Measured | Delta |
...

## Regressions
(entries with expected=native that did not classify as native)

## Errors
(entries whose query threw)

## Non-native entries (or All entries in --verbose)
(per-entry detail with plan summary, fallback_nodes, optional plan_tree)
```

The Markdown is intentionally compact for PR comment contexts; `--verbose` appends per-entry
detail and plan trees for the committed baseline.

---

## What it does NOT do

To anchor expectations honestly:

- **Does not compare query results** to vanilla Spark. Result correctness is the
  responsibility of the integration's existing test suites.
- **Does not measure execution time** or compare against a perf baseline. Performance gating
  is a separate concern.
- **Does not run a multi-axis matrix** (Spark x Integration x Scala) in one invocation. One
  environment per run; multiple environments require multiple runs.
- **Does not explain root causes** for fallbacks. Reports the `FallbackTag.reason` verbatim;
  classification, deduplication, and root-causing belong in a follow-on tool.
- **Does not measure operator-internal sub-expression coverage**. Plan nodes are the
  granularity; a node classified Native may have a Velox-side scalar expression that itself
  fell back to Spark via Gluten's expression-fallback path. Sub-expression coverage is a
  possible future extension but adds substantial complexity to the classifier.
- **Does not run on customer Spark event logs** (today). The input is a curated, authored
  matrix. An event-log input mode is a possible extension that would let the same classifier
  be fed plans from real-world workloads without re-execution.
- **Does not authoritatively cover everything**. The matrix is curated; a fallback in a code
  path the matrix does not exercise will not be detected. Adding entries is the right
  response.

---

## Module layout

```
gluten-coverage/
  pom.xml
  README.md                                              -- you are here
  src/main/scala/org/apache/gluten/coverage/
    NodeVerdict.scala                                    -- sealed trait + case classes
    PlanReport.scala                                     -- aggregate counts over a plan
    classifier/
      Classifier.scala                                   -- the decision tree
      CounterpartMap.scala                               -- vanilla-class -> "has counterpart" lookup
    matrix/
      MatrixDescriptor.scala                             -- SPI trait
      MatrixEntry.scala                                  -- row schema (Jackson-bound)
      MatrixLoader.scala                                 -- ServiceLoader + YAML parsing + validation
    listener/
      CoverageTags.scala                                 -- TreeNodeTag namespace
      PlanCollector.scala                                -- thread-safe per-entry plan accumulator
      CoverageQueryExecutionListener.scala               -- Spark listener -> classifier -> collector
    runner/
      RunnerOptions.scala                                -- CLI parsing
      CoverageRunner.scala                               -- main class
    report/
      Report.scala                                       -- output JSON model
      ReportBuilder.scala                                -- aggregator
    emitter/
      JsonEmitter.scala                                  -- Jackson-based JSON writer
      MarkdownEmitter.scala                              -- Markdown writer
      PlanTreeRenderer.scala                             -- tree + boundary rendering
      BaselineDiffer.scala                               -- gate check vs baseline
  src/test/scala/org/apache/gluten/coverage/
    classifier/ClassifierSuite.scala
    matrix/MatrixLoaderSuite.scala
    report/ReportBuilderSuite.scala
    emitter/EmitterSuite.scala
    emitter/PlanTreeRendererSuite.scala
```

---

## Building and testing

The module builds under the standard Gluten Maven layout. From the repo root:

```
./build/mvn -Pspark-3.5 -pl gluten-coverage install -DskipTests   # build
./build/mvn -Pspark-3.5 -pl gluten-coverage test                  # run unit tests
./build/mvn -Pspark-3.5 -pl gluten-coverage spotless:apply        # auto-format
```

The module compiles independently of any backend (no `-Pdelta` / `-Pbackends-velox` needed).
Each integration adds `gluten-coverage` as a `provided` dep and ships its own descriptor +
matrix YAML.
