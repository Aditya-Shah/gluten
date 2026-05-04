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

import org.apache.gluten.coverage.{AdapterDirection, ClassifiedNode, NodeVerdict, PlanReport}

import org.scalatest.funsuite.AnyFunSuite

class PlanTreeRendererSuite extends AnyFunSuite {

  test("renderTree shows depth-indented nodes with verdict markers") {
    val report = PlanReport(
      Seq(
        ClassifiedNode("WriteIntoDataSourceV2", 0, None, NodeVerdict.Vanilla("...")),
        ClassifiedNode(
          "VeloxColumnarToRowExec",
          1,
          Some(0),
          NodeVerdict
            .Adapter("VeloxColumnarToRowExec", AdapterDirection.ColumnarToRow, isTax = true)),
        ClassifiedNode("WholeStageTransformer", 2, Some(1), NodeVerdict.Native("...")),
        ClassifiedNode(
          "MergeIntoCommandEdge",
          1,
          Some(0),
          NodeVerdict.Fallback("MergeIntoCommandEdge", "MERGE INTO not yet offloaded"))
      )
    )
    val tree = PlanTreeRenderer.renderTree(report)
    val expected =
      """[V] WriteIntoDataSourceV2
        |  [A:tax] VeloxColumnarToRowExec
        |    [N] WholeStageTransformer
        |  [F] MergeIntoCommandEdge -- "MERGE INTO not yet offloaded"""".stripMargin
    assert(tree == expected, s"Got:\n$tree\n\nExpected:\n$expected")
  }

  test("renderTree handles empty plan") {
    assert(PlanTreeRenderer.renderTree(PlanReport.empty) == "(empty)")
  }

  test("boundaries returns deduped fallback nodes in walk order") {
    val report = PlanReport(
      Seq(
        ClassifiedNode("MergeIntoCommandEdge", 0, None, NodeVerdict.Fallback("Merge", "no merge")),
        ClassifiedNode("Project", 1, Some(0), NodeVerdict.Fallback("Project", "untagged")),
        // duplicate of above (opClass + reason both match) -- should be deduped
        ClassifiedNode("Project", 2, Some(1), NodeVerdict.Fallback("Project", "untagged")),
        ClassifiedNode("DeltaScan", 3, Some(2), NodeVerdict.Vanilla("DeltaScan"))
      )
    )
    val bs = PlanTreeRenderer.boundaries(report)
    assert(bs.size == 2)
    assert(bs.head.opClass == "MergeIntoCommandEdge")
    assert(bs.head.reason == "no merge")
    assert(bs(1).opClass == "Project")
    assert(bs(1).depth == 1) // first occurrence preserved
  }

  test("boundaries empty when plan is fully native") {
    val report = PlanReport(
      Seq(
        ClassifiedNode("Transformer", 0, None, NodeVerdict.Native("Transformer")),
        ClassifiedNode("Transformer2", 1, Some(0), NodeVerdict.Native("Transformer2"))
      )
    )
    assert(PlanTreeRenderer.boundaries(report).isEmpty)
  }
}
