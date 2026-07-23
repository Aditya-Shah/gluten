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
package org.apache.gluten.coverage.classifier

import org.apache.gluten.coverage.NodeVerdict
import org.apache.gluten.extension.columnar.{FallbackTag, FallbackTags}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.{FilterExec, InputAdapter, LocalTableScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the Classifier, exercising the decision tree against synthetic SparkPlan
 * instances. Native paths (TransformSupport, WholeStageTransformer) are exercised through
 * end-to-end integration tests in gluten-delta; this suite covers the JVM-only branches.
 */
class ClassifierSuite extends AnyFunSuite {

  private val intAttr: Attribute = AttributeReference("id", IntegerType)()
  private val strAttr: Attribute = AttributeReference("v", StringType)()

  private def localScan(): LocalTableScanExec =
    LocalTableScanExec(Seq(intAttr, strAttr), Seq.empty)

  test("empty children: vanilla scan classifies as Vanilla") {
    val plan = localScan()
    val report = Classifier().classify(plan)
    assert(report.nodeCount == 1)
    assert(report.nodes.head.verdict.isInstanceOf[NodeVerdict.Vanilla])
    assert(report.fallbackCount == 0)
    assert(report.nativeCount == 0)
    assert(report.isPureNative) // pure-native by definition since 0 fallbacks
  }

  test("FallbackTag-tagged node classifies as Fallback with the tag's reason") {
    val plan = localScan()
    FallbackTags.add(plan, FallbackTag.Appendable("test-reason"))
    val report = Classifier().classify(plan)
    assert(report.fallbackCount == 1)
    val fb = report.nodes.head.verdict.asInstanceOf[NodeVerdict.Fallback]
    assert(fb.reason == "test-reason")
    assert(report.fallbackReasons == Seq("test-reason"))
    assert(!report.isPureNative)
  }

  test("FallbackTag.Exclusive overwrites prior Appendable tags") {
    val plan = localScan()
    FallbackTags.add(plan, FallbackTag.Appendable("first"))
    FallbackTags.add(plan, FallbackTag.Exclusive("definitive"))
    val report = Classifier().classify(plan)
    val fb = report.nodes.head.verdict.asInstanceOf[NodeVerdict.Fallback]
    assert(fb.reason == "definitive")
  }

  test("fallback reason is recovered from the logicalLink when the physical tag was stripped") {
    // Gluten's RemoveFallbackTagRule strips physical FallbackTags before execution;
    // GlutenFallbackReporter mirrors the reason onto the logical link first.
    val child = localScan()
    val filter = FilterExec(Literal(true), child)
    val logical = LocalRelation(intAttr)
    FallbackTags.add(logical, FallbackTag.Appendable("native validation said no"))
    filter.setLogicalLink(logical)
    val report = Classifier().classify(filter)
    val fb = report.nodes.head.verdict.asInstanceOf[NodeVerdict.Fallback]
    assert(fb.reason == "native validation said no")
  }

  test("FilterExec without any tag classifies as Fallback with the no-reason default") {
    val child = localScan()
    val filter = FilterExec(Literal(true), child)
    val report = Classifier().classify(filter)
    val filterNode = report.nodes.head
    assert(filterNode.opClass == "FilterExec")
    assert(filterNode.verdict.isInstanceOf[NodeVerdict.Fallback])
    val reason = filterNode.verdict.asInstanceOf[NodeVerdict.Fallback].reason
    assert(reason == Classifier.NO_REASON_RECORDED)
  }

  test("codegen scaffolding classifies as Neutral and is descended through") {
    val child = localScan()
    val filter = FilterExec(Literal(true), child)
    val codegen = WholeStageCodegenExec(InputAdapter(filter))(codegenStageId = 1)
    val report = Classifier().classify(codegen)
    assert(
      report.nodes.map(_.opClass) ==
        Seq("WholeStageCodegenExec", "InputAdapter", "FilterExec", "LocalTableScanExec"))
    assert(report.neutralCount == 2)
    assert(report.fallbackCount == 1) // the FilterExec inside
    // Neutral nodes are excluded from actionable counts entirely.
    assert(report.nativeCount == 0)
    assert(report.vanillaCount == 1)
  }

  test("nodeName is captured alongside opClass for event-reason joining") {
    val filter = FilterExec(Literal(true), localScan())
    val report = Classifier().classify(filter)
    assert(report.nodes.head.opClass == "FilterExec")
    assert(report.nodes.head.nodeName == "Filter")
  }

  test("plan walk visits children in depth-first root-first order with depth tracking") {
    val child = localScan()
    val filter = FilterExec(Literal(true), child)
    val report = Classifier().classify(filter)
    assert(report.nodeCount == 2)
    assert(report.nodes(0).depth == 0)
    assert(report.nodes(0).opClass == "FilterExec")
    assert(report.nodes(1).depth == 1)
    assert(report.nodes(1).opClass == "LocalTableScanExec")
  }

  test("CounterpartMap recognises common vanilla classes") {
    assert(CounterpartMap.hasCounterpart("FilterExec"))
    assert(CounterpartMap.hasCounterpart("ProjectExec"))
    assert(CounterpartMap.hasCounterpart("HashAggregateExec"))
    assert(CounterpartMap.hasCounterpart("SortMergeJoinExec"))
    assert(!CounterpartMap.hasCounterpart("CompletelyMadeUpExec"))
    assert(!CounterpartMap.hasCounterpart("LocalTableScanExec"))
  }

  test("classifier never produces Unknown for a vanilla plan") {
    val plan = localScan()
    val report = Classifier().classify(plan)
    assert(report.unknownCount == 0)
  }

  test("isPureNative requires no fallback and no unknown nodes") {
    val plan = localScan()
    val report = Classifier().classify(plan)
    assert(report.isPureNative) // 0 fallback, 0 unknown -> pure-native (vacuously)

    val tagged = localScan()
    FallbackTags.add(tagged, FallbackTag.Appendable("r"))
    val report2 = Classifier().classify(tagged)
    assert(!report2.isPureNative)
  }
}
