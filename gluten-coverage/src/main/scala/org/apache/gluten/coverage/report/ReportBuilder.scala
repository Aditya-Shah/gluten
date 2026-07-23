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

import org.apache.gluten.coverage.classifier.ReasonNormalizer
import org.apache.gluten.coverage.emitter.PlanTreeRenderer
import org.apache.gluten.coverage.listener.{ExecutionPhase, ExecutionRecord, PlanCollector}
import org.apache.gluten.coverage.matrix.{BuildEnvironment, LoadedMatrix, MatrixEntry}

import java.time.Instant
import java.time.format.DateTimeFormatter

import scala.collection.mutable

object ReportBuilder {

  val SCHEMA_VERSION: Int = 2
  val TOOL_VERSION: String = "0.2.0"

  val DEFAULT_METADATA_WEIGHT: Double = 0.25

  case class EntryRunOutcome(
      entry: MatrixEntry,
      verdict: EntryVerdict,
      records: Seq[ExecutionRecord],
      rawJobs: Int,
      durationMs: Long,
      error: Option[String])

  def buildOutcome(
      entry: MatrixEntry,
      collector: PlanCollector,
      versionSkipped: Boolean): EntryRunOutcome = {
    if (versionSkipped) {
      return EntryRunOutcome(entry, EntryVerdict.Skipped, Seq.empty, 0, 0L, None)
    }
    val records = collector.recordsFor(entry.id)
    val error = collector
      .getError(entry.id)
      .orElse(records.flatMap(_.error).headOption)
    val countedSummary = summaryOf(records.filter(_.phase.countedInVerdict))
    val verdict =
      if (error.isDefined && countedSummary == PlanSummary.empty) EntryVerdict.Errored
      else EntryVerdict.of(countedSummary)
    EntryRunOutcome(
      entry = entry,
      verdict = verdict,
      records = records,
      rawJobs = collector.rawJobCount(entry.id),
      durationMs = records.map(_.durationMs).sum,
      error = error
    )
  }

  def build(
      matrix: LoadedMatrix,
      outcomes: Seq[EntryRunOutcome],
      env: BuildEnvironment,
      includePlanTree: Boolean = false,
      metadataWeight: Double = DEFAULT_METADATA_WEIGHT,
      unattributedExecutions: Int = 0): CoverageReport = {

    val sortedOutcomes = outcomes.sortBy(_.entry.id)

    val entryReports = sortedOutcomes.map(o => entryReport(o, includePlanTree))
    val summary = computeSummary(sortedOutcomes, metadataWeight, unattributedExecutions)
    val byFeature = sortedOutcomes
      .groupBy(_.entry.feature)
      .toSeq
      .sortBy(_._1)
      .map { case (feat, group) => featureSummary(feat, group) }
    val pareto = computePareto(sortedOutcomes)

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
      fallbackPareto = pareto,
      entries = entryReports,
      gateCheck = None
    )
  }

  // ---------------------------------------------------------------- per-entry

  private def entryReport(outcome: EntryRunOutcome, includePlanTree: Boolean): EntryReport = {
    val counted = outcome.records.filter(_.phase.countedInVerdict)
    val countedSummary = summaryOf(counted)
    val regression = outcome.verdict match {
      case EntryVerdict.Fallback | EntryVerdict.Partial => outcome.entry.expected == "native"
      case _ => false
    }
    val executionReports = outcome.records.map {
      r =>
        ExecutionReport(
          phase = r.phase.name,
          func = r.funcName,
          description = r.description,
          counted = r.phase.countedInVerdict,
          durationMs = r.durationMs,
          planSummary = r.planReport.map(PlanSummary.fromPlanReport).getOrElse(PlanSummary.empty),
          fallbackNodes = fallbackNodesOf(r),
          planTree = if (includePlanTree) r.planReport.map(PlanTreeRenderer.renderTree) else None,
          error = r.error
        )
    }
    val countedFallbackNodes = dedupeNodes(counted.flatMap(fallbackNodesOf))
    EntryReport(
      id = outcome.entry.id,
      feature = outcome.entry.feature,
      operation = outcome.entry.operation,
      dtype = outcome.entry.dtype,
      verdict = outcome.verdict.name,
      expected = outcome.entry.expected,
      regression = regression,
      durationMs = outcome.durationMs,
      planSummary = countedSummary,
      executions = executionReports,
      fallbackReasons = counted.flatMap(_.planReport).flatMap(_.fallbackReasons).distinct,
      fallbackNodes = countedFallbackNodes,
      rawJobsUncaptured = outcome.rawJobs,
      error = outcome.error
    )
  }

  private def fallbackNodesOf(record: ExecutionRecord): Seq[FallbackNodeJson] = {
    record.planReport
      .map(PlanTreeRenderer.boundaries)
      .getOrElse(Seq.empty)
      .map(
        b => FallbackNodeJson(b.opClass, b.depth, ReasonNormalizer.normalize(b.reason), b.reason))
  }

  private def dedupeNodes(nodes: Seq[FallbackNodeJson]): Seq[FallbackNodeJson] = {
    val seen = mutable.LinkedHashSet.empty[(String, String)]
    nodes.filter(n => seen.add((n.opClass, n.reason)))
  }

  // ---------------------------------------------------------------- summary

  private def summaryOf(records: Seq[ExecutionRecord]): PlanSummary = {
    records
      .flatMap(_.planReport)
      .map(PlanSummary.fromPlanReport)
      .foldLeft(PlanSummary.empty)(_.add(_))
  }

  private def computeSummary(
      outcomes: Seq[EntryRunOutcome],
      metadataWeight: Double,
      unattributedExecutions: Int): Summary = {
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

    val allRecords = outcomes.flatMap(_.records)
    val data = summaryOf(allRecords.filter(_.phase.countedInVerdict))
    val meta = summaryOf(allRecords.filter(_.phase == ExecutionPhase.DeltaMetadata))

    val dataDenominator = data.native + data.fallback + data.taxAdapters
    val metaDenominator = meta.native + meta.fallback + meta.taxAdapters
    val weightedDenominator = dataDenominator + metadataWeight * metaDenominator
    val nodeWeightedPercent =
      if (weightedDenominator == 0) 0.0
      else 100.0 * (data.native + metadataWeight * meta.native) / weightedDenominator
    val metadataNodeWeightedPercent =
      if (metaDenominator == 0) 0.0
      else 100.0 * meta.native / metaDenominator

    Summary(
      entryPureNativePercent = round1(entryPureNativePercent),
      nodeWeightedPercent = round1(nodeWeightedPercent),
      metadataNodeWeightedPercent = round1(metadataNodeWeightedPercent),
      metadataWeight = metadataWeight,
      entriesPureNative = pureNative,
      entriesPartial = partial,
      entriesFallback = fallback,
      entriesMetadata = metadata,
      entriesSkipped = skipped,
      entriesErrored = errored,
      entriesMultiExecution = outcomes.count(_.records.size >= 2),
      nodeNative = data.native,
      nodeFallback = data.fallback,
      nodeTaxAdapters = data.taxAdapters,
      nodeBenignAdapters = data.benignAdapters,
      nodeVanilla = data.vanilla,
      nodeNeutral = data.neutral,
      nodeUnknown = data.unknown,
      metadataNodeNative = meta.native,
      metadataNodeFallback = meta.fallback,
      metadataNodeTaxAdapters = meta.taxAdapters,
      unattributedExecutions = unattributedExecutions,
      rawJobsUncaptured = outcomes.map(_.rawJobs).sum
    )
  }

  private def featureSummary(feature: String, group: Seq[EntryRunOutcome]): FeatureSummary = {
    val pureNative = group.count(_.verdict == EntryVerdict.Native)
    val withFallback =
      group.count(o => o.verdict == EntryVerdict.Partial || o.verdict == EntryVerdict.Fallback)
    val total = pureNative + withFallback

    val records = group.flatMap(_.records)
    val counted = summaryOf(records.filter(_.phase.countedInVerdict))
    val scan = summaryOf(
      records.filter(
        r =>
          r.phase == ExecutionPhase.DmlScan || r.phase == ExecutionPhase.Query ||
            r.phase == ExecutionPhase.CommandWrapper))
    val write = summaryOf(records.filter(_.phase == ExecutionPhase.Write))

    FeatureSummary(
      feature = feature,
      entryPureNativePercent = round1(if (total == 0) 0.0 else 100.0 * pureNative / total),
      nodeWeightedPercent = round1(nodeWeighted(counted)),
      scanNodeWeightedPercent = round1(nodeWeighted(scan)),
      writeNodeWeightedPercent = round1(nodeWeighted(write)),
      entriesPureNative = pureNative,
      entriesWithFallback = withFallback,
      entriesTotal = total
    )
  }

  private def nodeWeighted(s: PlanSummary): Double = {
    val denominator = s.native + s.fallback + s.taxAdapters
    if (denominator == 0) 0.0 else 100.0 * s.native / denominator
  }

  // ---------------------------------------------------------------- pareto

  private case class ParetoAcc(
      entries: mutable.LinkedHashSet[String],
      features: mutable.LinkedHashSet[String],
      phases: mutable.Map[String, Int],
      executionDurations: mutable.Map[(String, Long), Long],
      var nodes: Int,
      var sampleReason: String)

  /**
   * Ranks (op_class, reason_code) pairs by the number of matrix entries they block, then by node
   * count. Includes every phase (metadata rows are visible, tagged by phase) so nothing is missed.
   */
  private def computePareto(outcomes: Seq[EntryRunOutcome]): Seq[ParetoRow] = {
    val acc = mutable.LinkedHashMap.empty[(String, String), ParetoAcc]
    outcomes.foreach {
      outcome =>
        outcome.records.foreach {
          record =>
            val fallbackNodes = record.planReport.map(_.nodes).getOrElse(Seq.empty).collect {
              case n if n.verdict.kind == "fallback" => n
            }
            fallbackNodes.foreach {
              node =>
                val reason = node.verdict match {
                  case f: org.apache.gluten.coverage.NodeVerdict.Fallback => f.reason
                  case _ => ""
                }
                val key = (node.opClass, ReasonNormalizer.normalize(reason))
                val a = acc.getOrElseUpdate(
                  key,
                  ParetoAcc(
                    mutable.LinkedHashSet.empty,
                    mutable.LinkedHashSet.empty,
                    mutable.Map.empty.withDefaultValue(0),
                    mutable.Map.empty,
                    0,
                    reason))
                a.entries += outcome.entry.id
                a.features += outcome.entry.feature
                a.phases(record.phase.name) = a.phases(record.phase.name) + 1
                a.executionDurations((outcome.entry.id, record.executionId)) = record.durationMs
                a.nodes += 1
            }
        }
    }
    acc.toSeq
      .map {
        case ((opClass, reasonCode), a) =>
          ParetoRow(
            opClass = opClass,
            reasonCode = reasonCode,
            sampleReason = a.sampleReason,
            entriesImpacted = a.entries.size,
            nodes = a.nodes,
            fallbackDurationMs = a.executionDurations.values.sum,
            features = a.features.toSeq.sorted,
            phases = a.phases.toMap,
            sampleEntries = a.entries.toSeq.sorted.take(3)
          )
      }
      .sortBy(r => (-r.entriesImpacted, -r.nodes, r.opClass, r.reasonCode))
  }

  private def round1(d: Double): Double = math.round(d * 10.0) / 10.0
}
