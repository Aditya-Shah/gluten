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

  private def nativeReport(): PlanReport = PlanReport(
    Seq(
      ClassifiedNode("FilterExecTransformer", 0, None, NodeVerdict.Native("FilterExecTransformer")),
      ClassifiedNode("DeltaScanTransformer", 1, Some(0), NodeVerdict.Native("DeltaScanTransformer"))
    )
  )

  private def fallbackReport(): PlanReport = PlanReport(
    Seq(
      ClassifiedNode("FilterExec", 0, None, NodeVerdict.Fallback("FilterExec", "untagged")),
      ClassifiedNode(
        "VeloxColumnarToRowExec",
        1,
        Some(0),
        NodeVerdict.Adapter("VeloxColumnarToRowExec", AdapterDirection.ColumnarToRow, isTax = true))
    )
  )

  private def partialReport(): PlanReport = PlanReport(
    Seq(
      ClassifiedNode("FilterExec", 0, None, NodeVerdict.Fallback("FilterExec", "ohno")),
      ClassifiedNode("DeltaScanTransformer", 1, Some(0), NodeVerdict.Native("DeltaScanTransformer"))
    )
  )

  test("EntryVerdict.of distinguishes native, partial, fallback, metadata") {
    assert(EntryVerdict.of(nativeReport()) == EntryVerdict.Native)
    assert(EntryVerdict.of(fallbackReport()) == EntryVerdict.Fallback)
    assert(EntryVerdict.of(partialReport()) == EntryVerdict.Partial)
    assert(EntryVerdict.of(PlanReport.empty) == EntryVerdict.Metadata)
  }

  test("entry-pure-native percent excludes metadata, skipped, errored from denominator") {
    val outcomes = Seq(
      ReportBuilder.EntryRunOutcome(
        entry("a", "native"),
        EntryVerdict.Native,
        Some(nativeReport()),
        1L,
        None),
      ReportBuilder.EntryRunOutcome(
        entry("b", "native"),
        EntryVerdict.Native,
        Some(nativeReport()),
        1L,
        None),
      ReportBuilder.EntryRunOutcome(
        entry("c", "fallback"),
        EntryVerdict.Fallback,
        Some(fallbackReport()),
        1L,
        None),
      ReportBuilder.EntryRunOutcome(
        entry("d", "fallback"),
        EntryVerdict.Partial,
        Some(partialReport()),
        1L,
        None),
      ReportBuilder.EntryRunOutcome(entry("e", "native"), EntryVerdict.Skipped, None, 0L, None),
      ReportBuilder.EntryRunOutcome(
        entry("f", "native"),
        EntryVerdict.Errored,
        None,
        0L,
        Some("boom"))
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
      ReportBuilder.EntryRunOutcome(
        entry("a", "native"),
        EntryVerdict.Partial,
        Some(partialReport()), // 1 native + 1 fallback
        1L,
        None)
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    // 1 native / (1 native + 1 fallback + 0 tax-adapter) = 50%
    assert(report.summary.nodeWeightedPercent == 50.0)
  }

  test("regression flagged when expected=native but verdict is fallback") {
    val outcomes = Seq(
      ReportBuilder.EntryRunOutcome(
        entry("a", "native"),
        EntryVerdict.Fallback,
        Some(fallbackReport()),
        1L,
        None)
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(report.entries.head.regression)
  }

  test("regression NOT flagged when expected=fallback and verdict matches") {
    val outcomes = Seq(
      ReportBuilder.EntryRunOutcome(
        entry("a", "fallback"),
        EntryVerdict.Fallback,
        Some(fallbackReport()),
        1L,
        None)
    )
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(!report.entries.head.regression)
  }

  test("entries are sorted by id deterministically") {
    val outcomes = Seq("z", "a", "m").map(
      id =>
        ReportBuilder.EntryRunOutcome(
          entry(id, "native"),
          EntryVerdict.Native,
          Some(nativeReport()),
          1L,
          None))
    val matrix = LoadedMatrix(descriptor, outcomes.map(_.entry))
    val report = ReportBuilder.build(matrix, outcomes, env())
    assert(report.entries.map(_.id) == Seq("a", "m", "z"))
  }

  private def env() = org.apache.gluten.coverage.matrix.BuildEnvironment(
    sparkVersion = "3.5.5",
    deltaVersion = "3.3.2",
    glutenVersion = "1.6.0",
    scalaBinaryVersion = "2.12",
    jdkVersion = "17",
    backend = "velox")
}
