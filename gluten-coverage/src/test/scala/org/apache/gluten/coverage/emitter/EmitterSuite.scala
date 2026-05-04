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
package org.apache.gluten.coverage.emitter

import org.apache.gluten.coverage.report._

import org.scalatest.funsuite.AnyFunSuite

class EmitterSuite extends AnyFunSuite {

  private val sample: CoverageReport = CoverageReport(
    schemaVersion = 1,
    toolVersion = "0.1.0",
    generatedAt = "2026-04-30T10:15:30Z",
    environment = EnvironmentReport(
      glutenVersion = "1.6.0",
      sparkVersion = "3.5.5",
      deltaVersion = "3.3.2",
      scalaBinaryVersion = "2.12",
      jdkVersion = "17",
      backend = "velox"),
    matrix = MatrixSummary(id = "delta", schemaVersion = 1, entryCount = 2),
    summary = Summary(
      entryPureNativePercent = 50.0,
      nodeWeightedPercent = 60.0,
      entriesPureNative = 1,
      entriesPartial = 0,
      entriesFallback = 1,
      entriesMetadata = 0,
      entriesSkipped = 0,
      entriesErrored = 0,
      nodeNative = 3,
      nodeFallback = 2,
      nodeTaxAdapters = 0,
      nodeBenignAdapters = 0,
      nodeVanilla = 0,
      nodeUnknown = 0
    ),
    byFeature = Seq(
      FeatureSummary("cow-read", 100.0, 100.0, 1, 0, 1),
      FeatureSummary("cow-write", 0.0, 0.0, 0, 1, 1)
    ),
    discrepancies = Seq.empty,
    entries = Seq(
      EntryReport(
        "a",
        "cow-read",
        "select",
        "int",
        "native",
        "native",
        regression = false,
        durationMs = 100L,
        planSummary = PlanSummary(3, 0, 0, 0, 0, 0),
        fallbackReasons = Seq.empty,
        fallbackNodes = Seq.empty,
        planTree = None,
        error = None
      ),
      EntryReport(
        "b",
        "cow-write",
        "merge",
        "int",
        "fallback",
        "fallback",
        regression = false,
        durationMs = 200L,
        planSummary = PlanSummary(0, 2, 0, 0, 0, 0),
        fallbackReasons = Seq("MERGE not yet offloaded"),
        fallbackNodes = Seq(FallbackNodeJson("MergeIntoCommandEdge", 0, "MERGE not yet offloaded")),
        planTree = None,
        error = None
      )
    ),
    gateCheck = None
  )

  test("JSON round-trip preserves all fields") {
    val json = JsonEmitter.emit(sample)
    val parsed = JsonEmitter.parse(json)
    assert(parsed.summary.entryPureNativePercent == 50.0)
    assert(parsed.summary.nodeWeightedPercent == 60.0)
    assert(parsed.entries.size == 2)
    assert(parsed.entries(1).fallbackReasons == Seq("MERGE not yet offloaded"))
  }

  test("JSON output uses snake_case field names") {
    val json = JsonEmitter.emit(sample)
    assert(json.contains("\"entry_pure_native_percent\""))
    assert(json.contains("\"node_weighted_percent\""))
    assert(json.contains("\"by_feature\""))
    assert(!json.contains("\"entryPureNativePercent\""))
  }

  test("Markdown output contains the headline metric") {
    val md = new MarkdownEmitter(verbose = false).emit(sample)
    assert(md.contains("# Delta on Gluten"))
    assert(md.contains("**50.0%**"))
    assert(md.contains("Node-weighted (detail) | 60.0%"))
    assert(md.contains("cow-read"))
    assert(md.contains("cow-write"))
  }

  test("BaselineDiffer flags an entry that regressed from native to fallback") {
    val baseline = sample.copy(
      entries = sample.entries.map(e => if (e.id == "b") e.copy(verdict = "native") else e),
      summary = sample.summary.copy(entryPureNativePercent = 100.0))
    val gate = BaselineDiffer.diff(sample, Some(baseline))
    assert(gate.regressions.contains("b"))
    assert(gate.entryPureNativeDeltaPercent == -50.0)
  }

  test("BaselineDiffer with no baseline returns no regressions") {
    val gate = BaselineDiffer.diff(sample, None)
    assert(gate.regressions.isEmpty)
  }
}
