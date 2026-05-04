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

import org.apache.gluten.coverage.report.{CoverageReport, EntryReport, GateCheck}

/**
 * Computes a [[GateCheck]] given the current report and an optional baseline.
 *
 * Regression rules (entry-pure-native gate):
 *   - An entry that was `native` in the baseline and is `partial` or `fallback` now is a
 *     regression.
 *   - An entry expected `native` in the matrix that is not `native` now is a regression (already
 *     flagged via `EntryReport.regression`).
 *
 * Improvements are not flagged as regressions; they are reflected in the delta percentage.
 */
object BaselineDiffer {

  def diff(current: CoverageReport, baselineOpt: Option[CoverageReport]): GateCheck = {
    val baseline = baselineOpt.getOrElse(emptyBaseline(current))

    val entryDelta = current.summary.entryPureNativePercent -
      baseline.summary.entryPureNativePercent
    val nodeDelta = current.summary.nodeWeightedPercent -
      baseline.summary.nodeWeightedPercent

    val baselineById = baseline.entries.map(e => e.id -> e).toMap
    val regressions: Seq[String] = current.entries
      .flatMap {
        entry =>
          baselineById.get(entry.id) match {
            case Some(prev) if isRegression(prev, entry) => Some(entry.id)
            case _ if entry.regression => Some(entry.id)
            case _ => None
          }
      }
      .distinct
      .sorted

    GateCheck(
      entryPureNativeDeltaPercent = round1(entryDelta),
      nodeWeightedDeltaPercent = round1(nodeDelta),
      regressions = regressions,
      toolFailures = Seq.empty
    )
  }

  private def isRegression(baseline: EntryReport, current: EntryReport): Boolean = {
    rank(baseline.verdict) > rank(current.verdict)
  }

  /**
   * Higher = better. Used so a verdict moving from a higher rank to a lower rank is a regression.
   */
  private def rank(verdict: String): Int = verdict match {
    case "native" => 4
    case "partial" => 3
    case "metadata" => 3 // metadata entries are not actionable; treat as neutral
    case "fallback" => 2
    case "error" => 1
    case "skipped" => 0
    case _ => 0
  }

  private def emptyBaseline(current: CoverageReport): CoverageReport =
    current.copy(
      summary = current.summary.copy(
        entryPureNativePercent = 0.0,
        nodeWeightedPercent = 0.0
      ),
      entries = Seq.empty
    )

  private def round1(d: Double): Double = math.round(d * 10.0) / 10.0
}
