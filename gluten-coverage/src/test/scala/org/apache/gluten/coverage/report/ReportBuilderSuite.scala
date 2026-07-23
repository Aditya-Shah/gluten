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

import org.apache.gluten.coverage.{AdapterDirection, ClassifiedNode, NodeVerdict, PlanReport}
import org.apache.gluten.coverage.listener.{ExecutionPhase, ExecutionRecord}
import org.apache.gluten.coverage.matrix.{LoadedMatrix, MatrixDescriptor, MatrixEntry}

import org.scalatest.funsuite.AnyFunSuite

class ReportBuilderSuite extends AnyFunSuite {

  private val descriptor: MatrixDescriptor = new MatrixDescriptor {
    override def id: String = "test"
    override def matrixResourcePath: String = "test.yaml"
  }

  private def entry(id: String, expected: String, feature: String = "f"): MatrixEntry =
    MatrixEntry(
      id = id,
      feature = feature,
      operation = "op",
      dtype = "int",
      expected = expected,
      upstreamClaim = None,
      deltaMinVersion = None,
      sparkMinVersion = None,
      setupSql = None,
      querySql = Some("SELECT 1"),
      teardownSql = None,
      config = None,
      scalaClass = None,
      notes = None,
      archived = None
    )

  private def node(opClass: String, verdict: NodeVerdict, depth: Int = 0): ClassifiedNode =
    ClassifiedNode(
      opClass,
      opClass.stripSuffix("Exec"),
      depth,
      if (depth == 0) None else Some(0),
      verdict)

  private def nativeReport(): PlanReport = PlanReport(
    Seq(
      node("FilterExecTransformer", NodeVerdict.Native("FilterExecTransformer")),
      node("DeltaScanTransformer", NodeVerdict.Native("DeltaScanTransformer"), 1)
    )
  )

  private def fallbackReport(reason: String = "untagged"): PlanReport = PlanReport(
    Seq(
      node("FilterExec", NodeVerdict.Fallback("FilterExec", reason)),
      node(
        "VeloxColumnarToRowExec",
        NodeVerdict.Adapter("VeloxColumnarToRowExec", AdapterDirection.ColumnarToRow, isTax = true),
        1)
    )
  )

  private def partialReport(): PlanReport = PlanReport(
    Seq(
      node("FilterExec", NodeVerdict.Fallback("FilterExec", "ohno")),
      node("DeltaScanTransformer", NodeVerdict.Native("DeltaScanTransformer"), 1)
    )
  )

  private def record(
      report: PlanReport,
      phase: ExecutionPhase = ExecutionPhase.Query,
      entryId: String = "e",
      executionId: Long = 1L,
      durationMs: Long = 10L): ExecutionRecord =
    ExecutionRecord(
      executionId = executionId,
      entryId = Some(entryId),
      phase = phase,
      funcName = Some("collect"),
      description = "desc",
      planReport = Some(report),
      durationMs = durationMs,
      error = None
    )

  private def outcome(
      e: MatrixEntry,
      verdict: EntryVerdict,
      records: Seq[ExecutionRecord],
      error: Option[String] = None): ReportBuilder.EntryRunOutcome =
    ReportBuilder.EntryRunOutcome(e, verdict, records, rawJobs = 0, durationMs = 1L, error = error)

  test("EntryVerdict.of distinguishes native, partial, fallback, metadata") {
    assert(EntryVerdict.of(nativeReport()) == EntryVerdict.Native)
    assert(EntryVerdict.of(fallbackReport()) == EntryVerdict.Fallback)
    assert(EntryVerdict.of(partialReport()) == EntryVerdict.Partial)
    assert(EntryVerdict.of(PlanReport.empty) == EntryVerdict.Metadata)
  }

  test("neutral nodes do not affect the entry verdict") {
    val wrappersOnly = PlanReport(
      Seq(
        node("AdaptiveSparkPlanExec", NodeVerdict.Neutral("AdaptiveSparkPlanExec")),
        node("ExecutedCommandExec", NodeVerdict.Neutral("ExecutedCommandExec"), 1)
      ))
    assert(EntryVerdict.of(wrappersOnly) == EntryVerdict.Metadata)
  }

  test("entry-pure-native percent excludes metadata, skipped, errored from denominator") {
    val outcomes = Seq(
      outcome(entry("a", "native"), EntryVerdict.Native, Seq(record(nativeReport()))),
      outcome(entry("b", "native"), EntryVerdict.Native, Seq(record(nativeReport()))),
      outcome(entry("c", "fallback"), EntryVerdict.Fallback, Seq(record(fallbackReport()))),
      outcome(entry("d", "fallback"), EntryVerdict.Partial, Seq(record(partialReport()))),
      outcome(entry("e", "native"), EntryVerdict.Skipped, Seq.empty),
      outcome(entry("f", "native"), EntryVerdict.Errored, Seq.empty, error = Some("boom"))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    // Denominator = native + partial + fallback = 4; pure-native = 2; 2/4 = 50%
    assert(report.summary.entryPureNativePercent == 50.0)
    assert(report.summary.entriesPureNative == 2)
    assert(report.summary.entriesPartial == 1)
    assert(report.summary.entriesFallback == 1)
    assert(report.summary.entriesSkipped == 1)
    assert(report.summary.entriesErrored == 1)
  }

  test("node-weighted percent excludes vanilla and benign adapters") {
    val outcomes = Seq(
      outcome(entry("a", "native"), EntryVerdict.Partial, Seq(record(partialReport())))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    // 1 native / (1 native + 1 fallback + 0 tax-adapter) = 50%
    assert(report.summary.nodeWeightedPercent == 50.0)
  }

  test("delta-metadata executions are down-weighted in node-weighted, tracked separately") {
    val outcomes = Seq(
      outcome(
        entry("a", "native"),
        EntryVerdict.Native,
        Seq(
          record(nativeReport(), ExecutionPhase.Query, executionId = 1L),
          record(fallbackReport(), ExecutionPhase.DeltaMetadata, executionId = 2L)
        )
      )
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env(), metadataWeight = 0.25)
    // Data-path: 2 native, 0 fallback. Metadata: 0 native, 1 fallback, 1 tax.
    // Weighted = (2 + 0.25*0) / (2 + 0.25*2) = 2/2.5 = 80%
    assert(report.summary.nodeWeightedPercent == 80.0)
    assert(report.summary.metadataNodeWeightedPercent == 0.0)
    assert(report.summary.metadataNodeFallback == 1)
    // Metadata fallback does not change the entry verdict.
    assert(report.entries.head.verdict == "native")
    // But the metadata execution is visible on the entry.
    assert(report.entries.head.executions.size == 2)
    assert(report.entries.head.executions.exists(e => e.phase == "delta-metadata" && !e.counted))
  }

  test("fallback pareto ranks (op_class, reason_code) by entries impacted, keeps duration") {
    val reason = "Deletion vector is not supported in native."
    val outcomes = Seq(
      outcome(
        entry("a", "native", feature = "mor-read"),
        EntryVerdict.Fallback,
        Seq(record(fallbackReport(reason), entryId = "a", executionId = 1L, durationMs = 100L))),
      outcome(
        entry("b", "native", feature = "mor-write"),
        EntryVerdict.Fallback,
        Seq(record(fallbackReport(reason), entryId = "b", executionId = 2L, durationMs = 50L))),
      outcome(
        entry("c", "native"),
        EntryVerdict.Fallback,
        Seq(record(fallbackReport("some other odd reason"), entryId = "c", executionId = 3L)))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    val pareto = report.fallbackPareto
    assert(pareto.nonEmpty)
    val top = pareto.head
    assert(top.opClass == "FilterExec")
    assert(top.reasonCode == "DELTA_DV_READ_NOT_SUPPORTED")
    assert(top.entriesImpacted == 2)
    assert(top.nodes == 2)
    assert(top.fallbackDurationMs == 150L)
    assert(top.features == Seq("mor-read", "mor-write"))
    assert(top.sampleEntries == Seq("a", "b"))
    val second = pareto(1)
    assert(second.reasonCode == "UNCLASSIFIED")
    assert(second.entriesImpacted == 1)
  }

  test("regression flagged when expected=native but verdict is fallback") {
    val outcomes = Seq(
      outcome(entry("a", "native"), EntryVerdict.Fallback, Seq(record(fallbackReport())))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(report.entries.head.regression)
  }

  test("regression NOT flagged when expected=fallback and verdict matches") {
    val outcomes = Seq(
      outcome(entry("a", "fallback"), EntryVerdict.Fallback, Seq(record(fallbackReport())))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(!report.entries.head.regression)
  }

  test("entries are sorted by id deterministically") {
    val outcomes = Seq("z", "a", "m").map(
      id => outcome(entry(id, "native"), EntryVerdict.Native, Seq(record(nativeReport()))))
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(report.entries.map(_.id) == Seq("a", "m", "z"))
  }

  test("multi-execution self-check counts entries with two or more captured executions") {
    val outcomes = Seq(
      outcome(
        entry("a", "native"),
        EntryVerdict.Native,
        Seq(
          record(nativeReport(), ExecutionPhase.DmlScan, executionId = 1L),
          record(nativeReport(), ExecutionPhase.Write, executionId = 2L))
      ),
      outcome(entry("b", "native"), EntryVerdict.Native, Seq(record(nativeReport())))
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(report.summary.entriesMultiExecution == 1)
  }

  private def env() = org.apache.gluten.coverage.matrix.BuildEnvironment(
    sparkVersion = "3.5.5",
    deltaVersion = "3.2.1",
    glutenVersion = "1.6.0",
    scalaBinaryVersion = "2.12",
    jdkVersion = "17",
    backend = "velox")
}
