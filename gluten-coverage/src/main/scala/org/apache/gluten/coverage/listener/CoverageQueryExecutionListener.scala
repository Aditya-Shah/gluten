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
package org.apache.gluten.coverage.listener

import org.apache.gluten.coverage.classifier.Classifier

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Listens to executed queries on a SparkSession. For each query whose `qe.logical` has been tagged
 * with `CoverageTags.ENTRY_ID`, classifies the executed plan and appends to the collector. Untagged
 * queries (e.g. setup/teardown SQL) are ignored.
 *
 * The listener fires on the same thread that executes the action (Spark's
 * `ExecutionListenerBus.post` is synchronous). Even so, the tag-on-plan approach is robust to any
 * future change in delivery semantics.
 */
class CoverageQueryExecutionListener(collector: PlanCollector) extends QueryExecutionListener {

  private val classifier: Classifier = Classifier()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val entryIdOpt = qe.logical.getTagValue(CoverageTags.ENTRY_ID)
    entryIdOpt.foreach {
      entryId =>
        try {
          val report = classifier.classify(qe.executedPlan)
          collector.addPlan(entryId, report, durationNs)
        } catch {
          case t: Throwable =>
            collector.addError(
              entryId,
              t.getClass.getSimpleName,
              s"classifier threw on success path: ${t.getMessage}")
        }
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val entryIdOpt = qe.logical.getTagValue(CoverageTags.ENTRY_ID)
    entryIdOpt.foreach {
      entryId => collector.addError(entryId, exception.getClass.getSimpleName, exception.getMessage)
    }
  }
}
