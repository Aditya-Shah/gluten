/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.coverage.report

import org.apache.gluten.coverage.PlanReport

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Top-level data model for the JSON output (schema v2). Field order matters for byte-deterministic
 * output.
 *
 * v2 changes vs v1: per-entry `executions` (a command entry captures several SQL executions, each
 * phase-labelled), `fallback_pareto` (the "what to fix first" ranking keyed by exact operator +
 * normalized reason), metadata weighting in the node-weighted metric, and removal of the
 * upstream-claim `discrepancies` machinery.
 */
case class CoverageReport(
    @JsonProperty("schema_version") schemaVersion: Int,
    @JsonProperty("tool_version") toolVersion: String,
    @JsonProperty("generated_at") generatedAt: String,
    environment: EnvironmentReport,
    matrix: MatrixSummary,
    summary: Summary,
    @JsonProperty("by_feature") byFeature: Seq[FeatureSummary],
    @JsonProperty("fallback_pareto") fallbackPareto: Seq[ParetoRow],
    entries: Seq[EntryReport],
    @JsonProperty("gate_check") gateCheck: Option[GateCheck]
)

case class EnvironmentReport(
    @JsonProperty("gluten_version") glutenVersion: String,
    @JsonProperty("spark_version") sparkVersion: String,
    @JsonProperty("delta_version") deltaVersion: String,
    @JsonProperty("scala_binary_version") scalaBinaryVersion: String,
    @JsonProperty("jdk_version") jdkVersion: String,
    backend: String
)

case class MatrixSummary(
    id: String,
    @JsonProperty("schema_version") schemaVersion: Int,
    @JsonProperty("entry_count") entryCount: Int
)

case class Summary(
    @JsonProperty("entry_pure_native_percent") entryPureNativePercent: Double,
    @JsonProperty("node_weighted_percent") nodeWeightedPercent: Double,
    @JsonProperty("metadata_node_weighted_percent") metadataNodeWeightedPercent: Double,
    @JsonProperty("metadata_weight") metadataWeight: Double,
    @JsonProperty("entries_pure_native") entriesPureNative: Int,
    @JsonProperty("entries_partial") entriesPartial: Int,
    @JsonProperty("entries_fallback") entriesFallback: Int,
    @JsonProperty("entries_metadata") entriesMetadata: Int,
    @JsonProperty("entries_skipped") entriesSkipped: Int,
    @JsonProperty("entries_errored") entriesErrored: Int,
    @JsonProperty("entries_multi_execution") entriesMultiExecution: Int,
    @JsonProperty("node_native") nodeNative: Int,
    @JsonProperty("node_fallback") nodeFallback: Int,
    @JsonProperty("node_tax_adapters") nodeTaxAdapters: Int,
    @JsonProperty("node_benign_adapters") nodeBenignAdapters: Int,
    @JsonProperty("node_vanilla") nodeVanilla: Int,
    @JsonProperty("node_neutral") nodeNeutral: Int,
    @JsonProperty("node_unknown") nodeUnknown: Int,
    @JsonProperty("metadata_node_native") metadataNodeNative: Int,
    @JsonProperty("metadata_node_fallback") metadataNodeFallback: Int,
    @JsonProperty("metadata_node_tax_adapters") metadataNodeTaxAdapters: Int,
    @JsonProperty("unattributed_executions") unattributedExecutions: Int,
    @JsonProperty("raw_jobs_uncaptured") rawJobsUncaptured: Int
)

case class FeatureSummary(
    feature: String,
    @JsonProperty("entry_pure_native_percent") entryPureNativePercent: Double,
    @JsonProperty("node_weighted_percent") nodeWeightedPercent: Double,
    @JsonProperty("scan_node_weighted_percent") scanNodeWeightedPercent: Double,
    @JsonProperty("write_node_weighted_percent") writeNodeWeightedPercent: Double,
    @JsonProperty("entries_pure_native") entriesPureNative: Int,
    @JsonProperty("entries_with_fallback") entriesWithFallback: Int,
    @JsonProperty("entries_total") entriesTotal: Int
)

/**
 * One row of the "what to fix first" ranking: an exact operator plus a normalized reason code,
 * ranked by the number of matrix entries it blocks (then node count). `fallback_duration_ms` is the
 * summed duration of the executions containing these fallback nodes -- an upper-bound proxy for
 * time impact, shown for context, not used for ranking.
 */
case class ParetoRow(
    @JsonProperty("op_class") opClass: String,
    @JsonProperty("reason_code") reasonCode: String,
    @JsonProperty("sample_reason") sampleReason: String,
    @JsonProperty("entries_impacted") entriesImpacted: Int,
    nodes: Int,
    @JsonProperty("fallback_duration_ms") fallbackDurationMs: Long,
    features: Seq[String],
    phases: Map[String, Int],
    @JsonProperty("sample_entries") sampleEntries: Seq[String]
)

case class EntryReport(
    id: String,
    feature: String,
    operation: String,
    dtype: String,
    verdict: String,
    expected: String,
    regression: Boolean,
    @JsonProperty("duration_ms") durationMs: Long,
    @JsonProperty("plan_summary") planSummary: PlanSummary,
    executions: Seq[ExecutionReport],
    @JsonProperty("fallback_reasons") fallbackReasons: Seq[String],
    @JsonProperty("fallback_nodes") fallbackNodes: Seq[FallbackNodeJson],
    @JsonProperty("raw_jobs_uncaptured") rawJobsUncaptured: Int,
    @JsonProperty("error") error: Option[String]
)

/** One captured SQL execution of an entry, phase-labelled. */
case class ExecutionReport(
    phase: String,
    func: Option[String],
    description: String,
    counted: Boolean,
    @JsonProperty("duration_ms") durationMs: Long,
    @JsonProperty("plan_summary") planSummary: PlanSummary,
    @JsonProperty("fallback_nodes") fallbackNodes: Seq[FallbackNodeJson],
    @JsonProperty("plan_tree") planTree: Option[String],
    @JsonProperty("error") error: Option[String]
)

case class FallbackNodeJson(
    @JsonProperty("op_class") opClass: String,
    depth: Int,
    @JsonProperty("reason_code") reasonCode: String,
    reason: String
)

case class PlanSummary(
    native: Int,
    fallback: Int,
    @JsonProperty("tax_adapters") taxAdapters: Int,
    @JsonProperty("benign_adapters") benignAdapters: Int,
    vanilla: Int,
    neutral: Int,
    unknown: Int
) {
  def add(other: PlanSummary): PlanSummary = PlanSummary(
    native = native + other.native,
    fallback = fallback + other.fallback,
    taxAdapters = taxAdapters + other.taxAdapters,
    benignAdapters = benignAdapters + other.benignAdapters,
    vanilla = vanilla + other.vanilla,
    neutral = neutral + other.neutral,
    unknown = unknown + other.unknown
  )
}

object PlanSummary {
  val empty: PlanSummary = PlanSummary(0, 0, 0, 0, 0, 0, 0)

  def fromPlanReport(report: PlanReport): PlanSummary = PlanSummary(
    native = report.nativeCount,
    fallback = report.fallbackCount,
    taxAdapters = report.taxAdapterCount,
    benignAdapters = report.benignAdapterCount,
    vanilla = report.vanillaCount,
    neutral = report.neutralCount,
    unknown = report.unknownCount
  )
}

case class GateCheck(
    @JsonProperty("entry_pure_native_delta_percent") entryPureNativeDeltaPercent: Double,
    @JsonProperty("node_weighted_delta_percent") nodeWeightedDeltaPercent: Double,
    regressions: Seq[String],
    @JsonProperty("tool_failures") toolFailures: Seq[String]
)

/** Per-entry classification used internally. Not directly serialized. */
sealed trait EntryVerdict {
  def name: String
}

object EntryVerdict {
  case object Native extends EntryVerdict { val name = "native" }
  case object Partial extends EntryVerdict { val name = "partial" }
  case object Fallback extends EntryVerdict { val name = "fallback" }
  case object Metadata extends EntryVerdict { val name = "metadata" }
  case object Skipped extends EntryVerdict { val name = "skipped" }
  case object Errored extends EntryVerdict { val name = "error" }

  def of(report: PlanReport): EntryVerdict = of(PlanSummary.fromPlanReport(report))

  def of(summary: PlanSummary): EntryVerdict = {
    val actionable = summary.native + summary.fallback + summary.taxAdapters + summary.unknown
    if (actionable == 0) Metadata
    else if (summary.fallback == 0 && summary.unknown == 0) Native
    else if (summary.native == 0) Fallback
    else Partial
  }
}
