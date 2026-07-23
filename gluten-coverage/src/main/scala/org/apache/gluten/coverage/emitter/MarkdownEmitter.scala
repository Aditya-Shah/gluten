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

import org.apache.gluten.coverage.report.{CoverageReport, EntryReport, FeatureSummary, GateCheck, ParetoRow}

import java.io.{File, FileWriter}

class MarkdownEmitter(verbose: Boolean) {

  def emit(report: CoverageReport): String = {
    val sb = new StringBuilder()
    appendHeader(sb, report)
    appendHeadline(sb, report)
    appendPareto(sb, report.fallbackPareto)
    appendByFeature(sb, report.byFeature)
    appendGate(sb, report.gateCheck)
    appendRegressions(sb, report.entries)
    appendErrors(sb, report.entries)
    if (verbose) {
      appendAllEntries(sb, report.entries)
    } else {
      appendNonNativeEntries(sb, report.entries)
    }
    sb.toString()
  }

  def emitTo(report: CoverageReport, path: File): Unit = {
    val parent = path.getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
    val w = new FileWriter(path)
    try w.write(emit(report))
    finally w.close()
  }

  private def appendHeader(sb: StringBuilder, report: CoverageReport): Unit = {
    sb.append(s"# ${report.matrix.id.capitalize} on Gluten - Coverage Report\n\n")
    sb.append(s"**Generated:** ${report.generatedAt}\n")
    sb.append(
      s"**Tool:** gluten-coverage ${report.toolVersion} (schema v${report.schemaVersion})\n")
    sb.append("**Environment:** ")
    sb.append(s"Spark ${report.environment.sparkVersion} + ")
    sb.append(s"${report.matrix.id.capitalize} ${report.environment.deltaVersion} + ")
    sb.append(s"Gluten ${report.environment.glutenVersion} + ")
    sb.append(s"Scala ${report.environment.scalaBinaryVersion} + ")
    sb.append(s"JDK ${report.environment.jdkVersion} (${report.environment.backend})\n\n")
  }

  private def appendHeadline(sb: StringBuilder, report: CoverageReport): Unit = {
    val s = report.summary
    val total = s.entriesPureNative + s.entriesPartial + s.entriesFallback
    sb.append("## Headline\n\n")
    sb.append("| Metric | Value |\n")
    sb.append("|---|---:|\n")
    sb.append(
      s"| **Entry-pure-native (headline)** | **${s.entryPureNativePercent}%** " +
        s"(${s.entriesPureNative} of $total) |\n")
    sb.append(s"| Node-weighted (metadata at x${s.metadataWeight}) | ${s.nodeWeightedPercent}% |\n")
    sb.append(s"| Delta-metadata node-weighted | ${s.metadataNodeWeightedPercent}% |\n")
    sb.append(
      s"| Entries: pure-native / partial / fallback | " +
        s"${s.entriesPureNative} / ${s.entriesPartial} / ${s.entriesFallback} |\n")
    sb.append(
      s"| Entries: metadata / skipped / errored | " +
        s"${s.entriesMetadata} / ${s.entriesSkipped} / ${s.entriesErrored} |\n")
    sb.append(
      s"| Nodes (data-path): native / fallback / tax-adapters | " +
        s"${s.nodeNative} / ${s.nodeFallback} / ${s.nodeTaxAdapters} |\n")
    sb.append(
      s"| Nodes (delta-metadata): native / fallback / tax-adapters | " +
        s"${s.metadataNodeNative} / ${s.metadataNodeFallback} / " +
        s"${s.metadataNodeTaxAdapters} |\n")
    sb.append(s"| Entries with >=2 captured executions | ${s.entriesMultiExecution} |\n")
    sb.append(
      s"| Unattributed executions / raw jobs | " +
        s"${s.unattributedExecutions} / ${s.rawJobsUncaptured} |\n")
    sb.append("\n")
    if (s.entriesMultiExecution == 0 && total > 0) {
      sb.append(
        "> **Attribution self-check failed:** no entry captured more than one execution. " +
          "Command entries (UPDATE/DELETE/MERGE/OPTIMIZE) should each capture several. " +
          "Check that the coverage listener is registered and the listener bus is draining.\n\n")
    }
  }

  private def appendPareto(sb: StringBuilder, pareto: Seq[ParetoRow]): Unit = {
    if (pareto.isEmpty) return
    sb.append("## What to fix first\n\n")
    sb.append(
      "Fallback (operator, reason) pairs ranked by entries impacted; " +
        "duration shown for context.\n\n")
    sb.append("| Operator | Reason | Entries | Nodes | Duration (ms) | Phases | Features |\n")
    sb.append("|---|---|---:|---:|---:|---|---|\n")
    pareto.take(15).foreach {
      row =>
        val phases = row.phases.toSeq.sortBy(_._1).map { case (p, n) => s"$p:$n" }.mkString(", ")
        sb.append(
          s"| `${row.opClass}` | ${row.reasonCode} | ${row.entriesImpacted} | ${row.nodes} | " +
            s"${row.fallbackDurationMs} | $phases | ${row.features.mkString(", ")} |\n")
    }
    sb.append("\n")
    pareto.take(15).foreach {
      row =>
        val quoted = "\"" + row.sampleReason + "\""
        sb.append(
          s"- **${row.reasonCode}** (`${row.opClass}`): $quoted -- " +
            s"e.g. ${row.sampleEntries.map(e => s"`$e`").mkString(", ")}\n")
    }
    sb.append("\n")
  }

  private def appendByFeature(sb: StringBuilder, features: Seq[FeatureSummary]): Unit = {
    if (features.isEmpty) return
    sb.append("## By feature\n\n")
    sb.append(
      "| Feature | Pure-native | Node-weighted | Scan | Write | " +
        "Entries (pure / fallback / total) |\n")
    sb.append("|---|---:|---:|---:|---:|---|\n")
    features.foreach {
      f =>
        sb.append(
          s"| ${f.feature} | ${f.entryPureNativePercent}% | ${f.nodeWeightedPercent}% | " +
            s"${f.scanNodeWeightedPercent}% | ${f.writeNodeWeightedPercent}% | " +
            s"${f.entriesPureNative} / ${f.entriesWithFallback} / ${f.entriesTotal} |\n")
    }
    sb.append("\n")
  }

  private def appendGate(sb: StringBuilder, gate: Option[GateCheck]): Unit = {
    gate.foreach {
      g =>
        sb.append("## Gate check\n\n")
        sb.append(s"- Entry-pure-native delta: ${formatDelta(g.entryPureNativeDeltaPercent)}\n")
        sb.append(s"- Node-weighted delta: ${formatDelta(g.nodeWeightedDeltaPercent)}\n")
        if (g.regressions.nonEmpty) {
          sb.append(s"- **Regressions** (${g.regressions.size}):\n")
          g.regressions.foreach(r => sb.append(s"  - `$r`\n"))
        } else {
          sb.append("- No regressions versus baseline.\n")
        }
        if (g.toolFailures.nonEmpty) {
          sb.append(s"- Tool-side failures (${g.toolFailures.size}):\n")
          g.toolFailures.foreach(f => sb.append(s"  - `$f`\n"))
        }
        sb.append("\n")
    }
  }

  private def appendRegressions(sb: StringBuilder, entries: Seq[EntryReport]): Unit = {
    val regressions = entries.filter(_.regression)
    if (regressions.isEmpty) return
    sb.append("## Regressions\n\n")
    sb.append("Entries marked `expected: native` that did not classify as native.\n\n")
    regressions.foreach {
      e =>
        sb.append(s"### `${e.id}`\n")
        sb.append(s"- Verdict: ${e.verdict} (expected: ${e.expected})\n")
        val plan = e.planSummary
        sb.append(
          s"- Plan: ${plan.native} native / ${plan.fallback} fallback / " +
            s"${plan.taxAdapters} tax-adapter\n")
        if (e.fallbackReasons.nonEmpty) {
          sb.append("- Fallback reasons:\n")
          e.fallbackReasons.foreach(r => sb.append(s"  - `$r`\n"))
        }
        sb.append("\n")
    }
  }

  private def appendErrors(sb: StringBuilder, entries: Seq[EntryReport]): Unit = {
    val errors = entries.filter(e => e.verdict == "error")
    if (errors.isEmpty) return
    sb.append("## Errors\n\n")
    errors.foreach(e => sb.append(s"- `${e.id}`: ${e.error.getOrElse("(no message)")}\n"))
    sb.append("\n")
  }

  private def appendNonNativeEntries(sb: StringBuilder, entries: Seq[EntryReport]): Unit = {
    val nonNative = entries.filter(e => e.verdict == "partial" || e.verdict == "fallback")
    if (nonNative.isEmpty) return
    sb.append("## Non-native entries\n\n")
    nonNative.foreach(e => appendEntryDetail(sb, e))
  }

  private def appendAllEntries(sb: StringBuilder, entries: Seq[EntryReport]): Unit = {
    if (entries.isEmpty) return
    sb.append("## All entries\n\n")
    entries.foreach(e => appendEntryDetail(sb, e))
  }

  private def appendEntryDetail(sb: StringBuilder, e: EntryReport): Unit = {
    sb.append(s"### `${e.id}`\n")
    sb.append(s"- Feature: ${e.feature} | Operation: ${e.operation} | Dtype: ${e.dtype}\n")
    sb.append(s"- Verdict: ${e.verdict} (expected: ${e.expected})\n")
    val p = e.planSummary
    sb.append(
      s"- Plan (counted executions): ${p.native} native / ${p.fallback} fallback / " +
        s"${p.taxAdapters} tax-adapter / ${p.benignAdapters} benign-adapter / " +
        s"${p.vanilla} vanilla / ${p.neutral} neutral\n")
    if (e.executions.nonEmpty) {
      sb.append(s"- Executions (${e.executions.size}):\n")
      e.executions.foreach {
        ex =>
          val ps = ex.planSummary
          val counted = if (ex.counted) "" else " (not counted)"
          val func = ex.func.map(f => s" `$f`").getOrElse("")
          sb.append(
            s"  - **${ex.phase}**$func$counted: ${ps.native}N/${ps.fallback}F/" +
              s"${ps.taxAdapters}T in ${ex.durationMs}ms\n")
          ex.fallbackNodes.foreach {
            fn =>
              val quoted = "\"" + fn.reason + "\""
              sb.append(s"    - `${fn.opClass}` [${fn.reasonCode}] -- $quoted\n")
          }
          ex.error.foreach(err => sb.append(s"    - error: $err\n"))
          ex.planTree.foreach {
            tree =>
              sb.append("    ```\n")
              tree.split('\n').foreach(line => sb.append("    ").append(line).append('\n'))
              sb.append("    ```\n")
          }
      }
    }
    if (e.rawJobsUncaptured > 0) {
      sb.append(s"- Raw jobs without SQL execution (not classified): ${e.rawJobsUncaptured}\n")
    }
    e.error.foreach(err => sb.append(s"- Error: $err\n"))
    sb.append("\n")
  }

  private def formatDelta(d: Double): String = {
    val sign = if (d > 0) "+" else ""
    s"$sign$d%"
  }
}
