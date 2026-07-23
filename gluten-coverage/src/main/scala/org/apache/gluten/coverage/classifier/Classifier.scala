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

import org.apache.gluten.coverage._
import org.apache.gluten.execution.{ColumnarToRowExecBase, GlutenPlan, RowToColumnarExecBase, TransformSupport, WholeStageTransformer}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.execution.{BaseSubqueryExec, CommandResultExec, InputAdapter, ReusedSubqueryExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import scala.collection.mutable.ArrayBuffer

/**
 * Walks an executed `SparkPlan` and classifies every node into one of: Native, Fallback, Adapter,
 * Vanilla, Neutral, or Unknown.
 *
 * Wrapper handling: AQE shells (`AdaptiveSparkPlanExec` -> `executedPlan`, `QueryStageExec` ->
 * `plan`), reuse brokers (`ReusedExchangeExec`, `ReusedSubqueryExec`), subquery brokers
 * (`BaseSubqueryExec`), codegen scaffolding (`WholeStageCodegenExec`, `InputAdapter`) and command
 * wrappers (`ExecutedCommandExec`, `CommandResultExec`) classify as Neutral and are descended
 * through (command wrappers are not descended: a V1 command body does its real work in separate SQL
 * executions which are captured independently). Subquery plans (`plan.subqueries`) are walked at
 * every node.
 *
 * Fallback reasons: Gluten's `RemoveFallbackTagRule` strips `FallbackTags` from the physical plan
 * before execution, so on an executed plan the reason survives only on the node's `logicalLink`
 * (mirrored there by `GlutenFallbackReporter`). The classifier reads the physical tag first (unit
 * tests, pre-strip plans), then the logical link, then falls back to the CounterpartMap default.
 *
 * Decision tree for regular nodes (in order):
 *   1. WholeStageTransformer / TransformSupport / other GlutenPlan -> Native. 2.
 *      ColumnarToRowExecBase / RowToColumnarExecBase -> Adapter (tax when parent not native). 3.
 *      FallbackTag on node or its logicalLink -> Fallback (reason from the tag). 4. CounterpartMap
 *      match -> Fallback ("no reason recorded"). 5. otherwise -> Vanilla.
 */
class Classifier {

  import Classifier.NO_REASON_RECORDED

  def classify(plan: SparkPlan): PlanReport = {
    val out = ArrayBuffer.empty[ClassifiedNode]
    walk(plan, depth = 0, parentIndex = None, parentIsNative = false, out)
    PlanReport(out.toSeq)
  }

  private def walk(
      plan: SparkPlan,
      depth: Int,
      parentIndex: Option[Int],
      parentIsNative: Boolean,
      out: ArrayBuffer[ClassifiedNode]): Unit = {
    val myIndex = out.length
    plan match {
      case a: AdaptiveSparkPlanExec =>
        out += neutral(a, depth, parentIndex)
        walk(a.executedPlan, depth + 1, Some(myIndex), parentIsNative, out)
      case q: QueryStageExec =>
        out += neutral(q, depth, parentIndex)
        walk(q.plan, depth + 1, Some(myIndex), parentIsNative, out)
      case r: ReusedExchangeExec =>
        out += neutral(r, depth, parentIndex)
        walk(r.child, depth + 1, Some(myIndex), parentIsNative, out)
      case r: ReusedSubqueryExec =>
        out += neutral(r, depth, parentIndex)
        walk(r.child, depth + 1, Some(myIndex), parentIsNative, out)
      case s: BaseSubqueryExec =>
        out += neutral(s, depth, parentIndex)
        s.children.foreach(c => walk(c, depth + 1, Some(myIndex), parentIsNative, out))
      case w: WholeStageCodegenExec =>
        out += neutral(w, depth, parentIndex)
        w.children.foreach(c => walk(c, depth + 1, Some(myIndex), parentIsNative, out))
      case i: InputAdapter =>
        out += neutral(i, depth, parentIndex)
        i.children.foreach(c => walk(c, depth + 1, Some(myIndex), parentIsNative, out))
      case c: CommandResultExec =>
        // The command already ran (in its own SQL execution, captured separately); this node just
        // replays cached rows.
        out += neutral(c, depth, parentIndex)
      case e: ExecutedCommandExec =>
        // V1 command wrapper. The command body spawns its own SQL executions (scans, writes),
        // which the listener captures and attributes independently; the wrapper itself is inert.
        out += neutral(e, depth, parentIndex)
      case _ =>
        val verdict = classifyNode(plan, parentIsNative)
        out += ClassifiedNode(
          plan.getClass.getSimpleName,
          plan.nodeName,
          depth,
          parentIndex,
          verdict)
        val isNative = verdict.isInstanceOf[NodeVerdict.Native]
        plan.children.foreach(child => walk(child, depth + 1, Some(myIndex), isNative, out))
    }
    plan.subqueries.foreach(sq => walk(sq, depth + 1, Some(myIndex), parentIsNative = false, out))
  }

  private def neutral(plan: SparkPlan, depth: Int, parentIndex: Option[Int]): ClassifiedNode =
    ClassifiedNode(
      plan.getClass.getSimpleName,
      plan.nodeName,
      depth,
      parentIndex,
      NodeVerdict.Neutral(plan.getClass.getSimpleName))

  private def classifyNode(plan: SparkPlan, parentIsNative: Boolean): NodeVerdict = {
    val opClass = plan.getClass.getSimpleName

    plan match {
      case _: WholeStageTransformer => NodeVerdict.Native(opClass)
      case _: TransformSupport => NodeVerdict.Native(opClass)
      case _: ColumnarToRowExecBase =>
        NodeVerdict.Adapter(opClass, AdapterDirection.ColumnarToRow, isTax = !parentIsNative)
      case _: RowToColumnarExecBase =>
        NodeVerdict.Adapter(opClass, AdapterDirection.RowToColumnar, isTax = !parentIsNative)
      case _: GlutenPlan => NodeVerdict.Native(opClass)
      case _ =>
        fallbackReason(plan) match {
          case Some(reason) => NodeVerdict.Fallback(opClass, reason)
          case None if CounterpartMap.hasCounterpart(opClass) =>
            NodeVerdict.Fallback(opClass, NO_REASON_RECORDED)
          case None => NodeVerdict.Vanilla(opClass)
        }
    }
  }

  /**
   * Physical tag first (present only on plans that never went through Gluten's final rules, e.g.
   * unit-test fixtures), then the logical link, where `GlutenFallbackReporter` mirrors the reason
   * before `RemoveFallbackTagRule` strips the physical tag.
   */
  private def fallbackReason(plan: SparkPlan): Option[String] = {
    FallbackTags
      .getOption(plan)
      .map(_.reason())
      .orElse(plan.logicalLink.flatMap(FallbackTags.getOption).map(_.reason()))
  }
}

object Classifier {

  /**
   * Reason recorded when a vanilla node has a known Gluten counterpart but no recoverable reason
   * (neither a FallbackTag nor a GlutenPlanFallbackEvent entry). Kept stable: the ReasonNormalizer
   * maps it to `NO_REASON_RECORDED`.
   */
  val NO_REASON_RECORDED: String = "no reason recorded (counterpart exists)"

  def apply(): Classifier = new Classifier()
}
