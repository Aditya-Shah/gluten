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

import org.apache.gluten.coverage.{ClassifiedNode, NodeVerdict, PlanReport}

/**
 * Renders a [[PlanReport]] into two complementary forms:
 *
 *   - `tree`: a depth-indented dump of the plan with a verdict marker per node, suitable for
 *     reading in a PR comment (verbose mode) or copying into a debugging note.
 *   - `boundaries`: a list of fallback nodes (where the native engine could not continue),
 *     deduplicated by (opClass, reason). The actionable one-liner for "what fell over".
 */
object PlanTreeRenderer {

  /**
   * Marker shorthand used in the rendered tree: [N] Native, [F] Fallback, [A:tax] tax-adapter,
   * [A:benign] benign-adapter, [V] Vanilla, [-] Neutral (wrapper/infrastructure), [?] Unknown.
   */
  def marker(verdict: NodeVerdict): String = verdict match {
    case _: NodeVerdict.Native => "[N]"
    case _: NodeVerdict.Fallback => "[F]"
    case a: NodeVerdict.Adapter => if (a.isTax) "[A:tax]" else "[A:benign]"
    case _: NodeVerdict.Vanilla => "[V]"
    case _: NodeVerdict.Neutral => "[-]"
    case _: NodeVerdict.Unknown => "[?]"
  }

  /**
   * Render the plan as an indented tree. Each line is `<indent><marker> <opClass>` plus, for
   * fallback nodes, the reason in quotes.
   */
  def renderTree(report: PlanReport): String = {
    if (report.nodes.isEmpty) return "(empty)"
    val sb = new StringBuilder()
    report.nodes.foreach {
      node =>
        val indent = "  " * node.depth
        sb.append(indent).append(marker(node.verdict)).append(' ').append(node.opClass)
        node.verdict match {
          case f: NodeVerdict.Fallback => sb.append(" -- \"").append(f.reason).append('"')
          case _ => ()
        }
        sb.append('\n')
    }
    sb.toString().stripSuffix("\n")
  }

  /**
   * Extract fallback nodes as machine-friendly tuples. The order is the depth-first walk order;
   * deduplication is by (opClass, reason) so a fallback parent with many fallback descendants does
   * not flood the report.
   */
  case class FallbackBoundary(opClass: String, depth: Int, reason: String)

  def boundaries(report: PlanReport): Seq[FallbackBoundary] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[(String, String)]
    val out = scala.collection.mutable.ArrayBuffer.empty[FallbackBoundary]
    report.nodes.foreach {
      case ClassifiedNode(opClass, _, depth, _, NodeVerdict.Fallback(_, reason)) =>
        val key = (opClass, reason)
        if (!seen.contains(key)) {
          seen.add(key)
          out += FallbackBoundary(opClass, depth, reason)
        }
      case _ =>
    }
    out.toSeq
  }
}
