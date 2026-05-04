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

/** Top-level data model for the JSON output. Field order matters for byte-deterministic output. */
case class CoverageReport(
    @JsonProperty("schema_version") schemaVersion: Int,
    @JsonProperty("tool_version") toolVersion: String,
    @JsonProperty("generated_at") generatedAt: String,
    environment: EnvironmentReport,
    matrix: MatrixSummary,
    summary: Summary,
    @JsonProperty("by_feature") byFeature: Seq[FeatureSummary],
    discrepancies: Seq[Discrepancy],
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
    @JsonProperty("entries_pure_native") entriesPureNative: Int,
    @JsonProperty("entries_partial") entriesPartial: Int,
    @JsonProperty("entries_fallback") entriesFallback: Int,
    @JsonProperty("entries_metadata") entriesMetadata: Int,
    @JsonProperty("entries_skipped") entriesSkipped: Int,
    @JsonProperty("entries_errored") entriesErrored: Int,
    @JsonProperty("node_native") nodeNative: Int,
    @JsonProperty("node_fallback") nodeFallback: Int,
    @JsonProperty("node_tax_adapters") nodeTaxAdapters: Int,
    @JsonProperty("node_benign_adapters") nodeBenignAdapters: Int,
    @JsonProperty("node_vanilla") nodeVanilla: Int,
    @JsonProperty("node_unknown") nodeUnknown: Int
)

case class FeatureSummary(
    feature: String,
    @JsonProperty("entry_pure_native_percent") entryPureNativePercent: Double,
    @JsonProperty("node_weighted_percent") nodeWeightedPercent: Double,
    @JsonProperty("entries_pure_native") entriesPureNative: Int,
    @JsonProperty("entries_with_fallback") entriesWithFallback: Int,
    @JsonProperty("entries_total") entriesTotal: Int
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
    @JsonProperty("fallback_reasons") fallbackReasons: Seq[String],
    @JsonProperty("fallback_nodes") fallbackNodes: Seq[FallbackNodeJson],
    @JsonProperty("plan_tree") planTree: Option[String],
    @JsonProperty("error") error: Option[String]
)

case class FallbackNodeJson(
    @JsonProperty("op_class") opClass: String,
    depth: Int,
    reason: String
)

case class PlanSummary(
    native: Int,
    fallback: Int,
    @JsonProperty("tax_adapters") taxAdapters: Int,
    @JsonProperty("benign_adapters") benignAdapters: Int,
    vanilla: Int,
    unknown: Int
) {
  def fromPlanReport(): PlanSummary = this
}

object PlanSummary {
  val empty: PlanSummary = PlanSummary(0, 0, 0, 0, 0, 0)

  def fromPlanReport(report: PlanReport): PlanSummary = PlanSummary(
    native = report.nativeCount,
    fallback = report.fallbackCount,
    taxAdapters = report.taxAdapterCount,
    benignAdapters = report.benignAdapterCount,
    vanilla = report.vanillaCount,
    unknown = report.unknownCount
  )
}

case class Discrepancy(
    id: String,
    @JsonProperty("upstream_claim") upstreamClaim: String,
    measured: String,
    delta: String
)

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

  def of(report: PlanReport): EntryVerdict = {
    val actionable =
      report.nativeCount + report.fallbackCount + report.taxAdapterCount + report.unknownCount
    if (actionable == 0) Metadata
    else if (report.fallbackCount == 0 && report.unknownCount == 0) Native
    else if (report.nativeCount == 0) Fallback
    else Partial
  }
}
