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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import scala.util.control.NonFatal

/**
 * The coverage tool's event tap, registered on the Spark listener bus (`sc.addSparkListener`).
 *
 * Unlike a `QueryExecutionListener` -- which Spark only notifies for *named* executions (Dataset
 * actions and eager command execution) -- this listener receives every
 * `SparkListenerSQLExecutionStart`/`End` pair, including Delta's unnamed nested executions, with
 * the live `QueryExecution` attached to the end event. It also captures `GlutenPlanFallbackEvent`
 * (per-execution fallback reasons posted by `GlutenFallbackReporter`) and raw job starts that have
 * no SQL execution at all (e.g. MERGE source materialization).
 *
 * `GlutenPlanFallbackEvent` is consumed reflectively so the module needs no gluten-ui dependency
 * and degrades gracefully when the Gluten UI is disabled or absent.
 */
class CoverageSparkListener(collector: PlanCollector) extends SparkListener {

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case s: SparkListenerSQLExecutionStart => collector.onExecutionStart(s)
    case e: SparkListenerSQLExecutionEnd => collector.onExecutionEnd(e)
    case other if other.getClass.getSimpleName == "GlutenPlanFallbackEvent" =>
      handleGlutenFallbackEvent(other)
    case _ =>
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val props = jobStart.properties
    if (props != null && props.getProperty("spark.sql.execution.id") == null) {
      collector.onJobStartWithoutExecution(props)
    }
  }

  private def handleGlutenFallbackEvent(event: SparkListenerEvent): Unit = {
    try {
      val cls = event.getClass
      val executionId = cls.getMethod("executionId").invoke(event).asInstanceOf[Long]
      val reasons = cls
        .getMethod("fallbackNodeToReason")
        .invoke(event)
        .asInstanceOf[Map[String, String]]
      collector.onGlutenPlanFallback(executionId, reasons)
    } catch {
      case NonFatal(_) => // Unknown event shape from a different Gluten version; skip.
    }
  }
}
