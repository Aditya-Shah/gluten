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

import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ArrayBuffer

/**
 * Walks an executed `SparkPlan` and classifies every node into one of: Native, Fallback, Adapter,
 * Vanilla, or Unknown.
 *
 * The decision tree (in order):
 *   1. FallbackTags non-empty -> Fallback (reason from the tag) 2. WholeStageTransformer -> Native
 *      (wraps a native region) 3. TransformSupport -> Native (individual transformer) 4.
 *      ColumnarToRowExecBase -> Adapter (Columnar -> Row) 5. RowToColumnarExecBase -> Adapter (Row
 *      -> Columnar) 6. GlutenPlan (other) -> Native (rare; non-TransformSupport Gluten node) 7.
 *      CounterpartMap match -> Fallback (untagged; defensive) 8. otherwise -> Vanilla
 *
 * Adapter nodes are tax-adapters when their parent is not Native (the adapter pair crosses a JVM
 * region) and benign-adapters otherwise.
 */
class Classifier {

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
    val verdict = classifyNode(plan, parentIsNative)
    val myIndex = out.length
    out += ClassifiedNode(plan.getClass.getSimpleName, depth, parentIndex, verdict)
    val isNative = verdict.isInstanceOf[NodeVerdict.Native]
    plan.children.foreach(child => walk(child, depth + 1, Some(myIndex), isNative, out))
  }

  private def classifyNode(plan: SparkPlan, parentIsNative: Boolean): NodeVerdict = {
    val opClass = plan.getClass.getSimpleName

    if (FallbackTags.nonEmpty(plan)) {
      val reason = FallbackTags.getOption(plan).map(_.reason()).getOrElse("untagged")
      NodeVerdict.Fallback(opClass, reason)
    } else if (plan.isInstanceOf[WholeStageTransformer]) {
      NodeVerdict.Native(opClass)
    } else if (plan.isInstanceOf[TransformSupport]) {
      NodeVerdict.Native(opClass)
    } else if (plan.isInstanceOf[ColumnarToRowExecBase]) {
      NodeVerdict.Adapter(opClass, AdapterDirection.ColumnarToRow, isTax = !parentIsNative)
    } else if (plan.isInstanceOf[RowToColumnarExecBase]) {
      NodeVerdict.Adapter(opClass, AdapterDirection.RowToColumnar, isTax = !parentIsNative)
    } else if (plan.isInstanceOf[GlutenPlan]) {
      NodeVerdict.Native(opClass)
    } else if (CounterpartMap.hasCounterpart(opClass)) {
      NodeVerdict.Fallback(opClass, "untagged-fallback (counterpart exists)")
    } else {
      NodeVerdict.Vanilla(opClass)
    }
  }
}

object Classifier {
  def apply(): Classifier = new Classifier()
}
