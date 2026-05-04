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
import org.apache.gluten.coverage.emitter.PlanTreeRenderer
import org.apache.gluten.coverage.listener.PlanCollector
import org.apache.gluten.coverage.matrix.{BuildEnvironment, LoadedMatrix, MatrixEntry}

import java.time.Instant
import java.time.format.DateTimeFormatter

object ReportBuilder {

  val SCHEMA_VERSION: Int = 1
  val TOOL_VERSION: String = "0.1.0"

  case class EntryRunOutcome(
      entry: MatrixEntry,
      verdict: EntryVerdict,
      planReport: Option[PlanReport],
      durationMs: Long,
      error: Option[String])

  def buildOutcome(
      entry: MatrixEntry,
      collector: PlanCollector,
      env: BuildEnvironment,
      versionSkipped: Boolean): EntryRunOutcome = {
    if (versionSkipped) {
      EntryRunOutcome(entry, EntryVerdict.Skipped, None, 0L, None)
    } else if (collector.hasError(entry.id) && !collector.hasPlan(entry.id)) {
      EntryRunOutcome(
        entry = entry,
        verdict = EntryVerdict.Errored,
        planReport = None,
        durationMs = collector.getDurationNs(entry.id) / 1000000L,
        error = collector.getError(entry.id))
    } else {
      val planOpt = collector.getPlan(entry.id)
      val verdict = planOpt.map(EntryVerdict.of).getOrElse(EntryVerdict.Metadata)
      EntryRunOutcome(
        entry = entry,
        verdict = verdict,
        planReport = planOpt,
        durationMs = collector.getDurationNs(entry.id) / 1000000L,
        error = collector.getError(entry.id))
    }
  }

  def build(
      matrix: LoadedMatrix,
      outcomes: Seq[EntryRunOutcome],
      env: BuildEnvironment,
      includePlanTree: Boolean = false): CoverageReport = {

    val sortedOutcomes = outcomes.sortBy(_.entry.id)

    val entryReports = sortedOutcomes.map {
      outcome =>
        val planSummary = outcome.planReport
          .map(PlanSummary.fromPlanReport)
          .getOrElse(PlanSummary.empty)
        val regression = outcome.verdict match {
          case EntryVerdict.Fallback | EntryVerdict.Partial =>
            outcome.entry.expected == "native"
          case _ => false
        }
        val fallbackNodes = outcome.planReport
          .map(PlanTreeRenderer.boundaries)
          .getOrElse(Seq.empty)
          .map(b => FallbackNodeJson(b.opClass, b.depth, b.reason))
        // Plan tree is only included when explicitly requested, since it bloats the JSON for
        // large matrices. Boundaries are always included; they are the actionable subset.
        val planTree = if (includePlanTree) {
          outcome.planReport.map(PlanTreeRenderer.renderTree)
        } else None
        EntryReport(
          id = outcome.entry.id,
          feature = outcome.entry.feature,
          operation = outcome.entry.operation,
          dtype = outcome.entry.dtype,
          verdict = outcome.verdict.name,
          expected = outcome.entry.expected,
          regression = regression,
          durationMs = outcome.durationMs,
          planSummary = planSummary,
          fallbackReasons = outcome.planReport.map(_.fallbackReasons).getOrElse(Seq.empty),
          fallbackNodes = fallbackNodes,
          planTree = planTree,
          error = outcome.error
        )
    }

    val summary = computeSummary(sortedOutcomes)

    val byFeature = sortedOutcomes
      .groupBy(_.entry.feature)
      .toSeq
      .sortBy(_._1)
      .map { case (feat, group) => featureSummary(feat, group) }

    val discrepancies = computeDiscrepancies(sortedOutcomes, matrix)

    CoverageReport(
      schemaVersion = SCHEMA_VERSION,
      toolVersion = TOOL_VERSION,
      generatedAt = DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
      environment = EnvironmentReport(
        glutenVersion = env.glutenVersion,
        sparkVersion = env.sparkVersion,
        deltaVersion = env.deltaVersion,
        scalaBinaryVersion = env.scalaBinaryVersion,
        jdkVersion = env.jdkVersion,
        backend = env.backend
      ),
      matrix = MatrixSummary(
        id = matrix.descriptor.id,
        schemaVersion = SCHEMA_VERSION,
        entryCount = matrix.entries.size
      ),
      summary = summary,
      byFeature = byFeature,
      discrepancies = discrepancies,
      entries = entryReports,
      gateCheck = None
    )
  }

  private def computeSummary(outcomes: Seq[EntryRunOutcome]): Summary = {
    val pureNative = outcomes.count(_.verdict == EntryVerdict.Native)
    val partial = outcomes.count(_.verdict == EntryVerdict.Partial)
    val fallback = outcomes.count(_.verdict == EntryVerdict.Fallback)
    val metadata = outcomes.count(_.verdict == EntryVerdict.Metadata)
    val skipped = outcomes.count(_.verdict == EntryVerdict.Skipped)
    val errored = outcomes.count(_.verdict == EntryVerdict.Errored)

    val actionableEntries = pureNative + partial + fallback
    val entryPureNativePercent =
      if (actionableEntries == 0) 0.0
      else 100.0 * pureNative / actionableEntries

    val totalNative = outcomes.flatMap(_.planReport).map(_.nativeCount).sum
    val totalFallback = outcomes.flatMap(_.planReport).map(_.fallbackCount).sum
    val totalTaxAdapters = outcomes.flatMap(_.planReport).map(_.taxAdapterCount).sum
    val totalBenignAdapters = outcomes.flatMap(_.planReport).map(_.benignAdapterCount).sum
    val totalVanilla = outcomes.flatMap(_.planReport).map(_.vanillaCount).sum
    val totalUnknown = outcomes.flatMap(_.planReport).map(_.unknownCount).sum

    val nodeDenominator = totalNative + totalFallback + totalTaxAdapters
    val nodeWeightedPercent =
      if (nodeDenominator == 0) 0.0
      else 100.0 * totalNative / nodeDenominator

    Summary(
      entryPureNativePercent = round1(entryPureNativePercent),
      nodeWeightedPercent = round1(nodeWeightedPercent),
      entriesPureNative = pureNative,
      entriesPartial = partial,
      entriesFallback = fallback,
      entriesMetadata = metadata,
      entriesSkipped = skipped,
      entriesErrored = errored,
      nodeNative = totalNative,
      nodeFallback = totalFallback,
      nodeTaxAdapters = totalTaxAdapters,
      nodeBenignAdapters = totalBenignAdapters,
      nodeVanilla = totalVanilla,
      nodeUnknown = totalUnknown
    )
  }

  private def featureSummary(feature: String, group: Seq[EntryRunOutcome]): FeatureSummary = {
    val pureNative = group.count(_.verdict == EntryVerdict.Native)
    val withFallback =
      group.count(o => o.verdict == EntryVerdict.Partial || o.verdict == EntryVerdict.Fallback)
    val total = pureNative + withFallback

    val totalNative = group.flatMap(_.planReport).map(_.nativeCount).sum
    val totalFallback = group.flatMap(_.planReport).map(_.fallbackCount).sum
    val totalTaxAdapters = group.flatMap(_.planReport).map(_.taxAdapterCount).sum

    val entryPureNativePercent =
      if (total == 0) 0.0 else 100.0 * pureNative / total

    val nodeDenominator = totalNative + totalFallback + totalTaxAdapters
    val nodeWeightedPercent =
      if (nodeDenominator == 0) 0.0 else 100.0 * totalNative / nodeDenominator

    FeatureSummary(
      feature = feature,
      entryPureNativePercent = round1(entryPureNativePercent),
      nodeWeightedPercent = round1(nodeWeightedPercent),
      entriesPureNative = pureNative,
      entriesWithFallback = withFallback,
      entriesTotal = total
    )
  }

  private def computeDiscrepancies(
      outcomes: Seq[EntryRunOutcome],
      matrix: LoadedMatrix): Seq[Discrepancy] = {
    outcomes.flatMap {
      outcome =>
        val claim = outcome.entry.upstreamClaim
        claim match {
          case None => None
          case Some(c) =>
            val measured = outcome.verdict.name
            val delta = (c, measured) match {
              case ("yes", "native") => None
              case ("no", "fallback") => None
              case ("partial", "partial") => None
              case ("not-tested", _) => Some("upstream-not-tested")
              case ("yes", _) => Some("regression-vs-doc")
              case ("partial", "native") => Some("better-than-doc")
              case ("partial", "fallback") => Some("worse-than-doc")
              case ("no", "native") => Some("better-than-doc")
              case ("no", "partial") => Some("partial-vs-doc")
              case _ => Some("unclassified")
            }
            delta.map(d => Discrepancy(outcome.entry.id, c, measured, d))
        }
    }
  }

  private def round1(d: Double): Double = math.round(d * 10.0) / 10.0
}
