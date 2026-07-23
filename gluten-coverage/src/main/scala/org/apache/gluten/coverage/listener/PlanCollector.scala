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

import org.apache.gluten.coverage.{ClassifiedNode, NodeVerdict, PlanReport}
import org.apache.gluten.coverage.classifier.Classifier

import org.apache.spark.sql.coverage.SqlEventAccess
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Accumulates captured SQL executions attributed to matrix entries.
 *
 * Attribution of an execution, most-precise layer first:
 *   1. Job tag: the runner sets `gluten-coverage:<entryId>` via `sc.addJobTag`; the tag arrives on
 *      `SparkListenerSQLExecutionStart.jobTags` (and `spark.job.tags` in job properties). Survives
 *      any thread that carries the caller's local properties, including Delta's forwarding pools.
 *      2. Root-execution-id chaining: an execution whose `rootExecutionId` (transitively) maps to
 *      an attributed execution inherits its entry. 3. Open window: the runner runs entries strictly
 *      sequentially and drains the listener bus at the window edges, so anything else starting
 *      inside the window belongs to the entry. This is the layer that catches OPTIMIZE's fork-join
 *      bin writes, where tag and root-id inheritance are best-effort.
 *
 * Executions matching no layer land in the unattributed bucket -- never on the wrong entry.
 *
 * Everything is thread-safe: events arrive on the listener-bus thread while the runner drives
 * windows from the main thread. Plans are classified at end-event time and the `QueryExecution`
 * reference is dropped immediately.
 */
class PlanCollector(classifier: Classifier = Classifier()) {

  import PlanCollector._

  @volatile private var window: Option[String] = None

  /** executionId -> attributed entry id; retained across entries for root-id chaining. */
  private val executionEntry = new ConcurrentHashMap[Long, String]()
  private val executionStarts = new ConcurrentHashMap[Long, SparkListenerSQLExecutionStart]()

  /** executionId -> merged fallbackNodeToReason from GlutenPlanFallbackEvent(s) (one per stage). */
  private val glutenReasons = new ConcurrentHashMap[Long, Map[String, String]]()

  private val records = new ConcurrentHashMap[String, ArrayBuffer[ExecutionRecord]]()
  private val unattributed = new ArrayBuffer[ExecutionRecord]()
  private val rawJobs = new ConcurrentHashMap[String, Integer]()
  private val errors = new ConcurrentHashMap[String, String]()

  // ---------------------------------------------------------------- runner-facing API

  def openWindow(entryId: String): Unit = { window = Some(entryId) }

  def closeWindow(): Unit = { window = None }

  def addError(entryId: String, errorClass: String, message: String): Unit = {
    errors.put(entryId, s"$errorClass: $message")
  }

  def recordsFor(entryId: String): Seq[ExecutionRecord] = {
    Option(records.get(entryId)).map(b => b.synchronized(b.toSeq)).getOrElse(Seq.empty)
  }

  def rawJobCount(entryId: String): Int = rawJobs.getOrDefault(entryId, 0)

  def getError(entryId: String): Option[String] = Option(errors.get(entryId))

  def unattributedRecords: Seq[ExecutionRecord] = unattributed.synchronized(unattributed.toSeq)

  def clear(): Unit = {
    executionEntry.clear()
    executionStarts.clear()
    glutenReasons.clear()
    records.clear()
    unattributed.synchronized(unattributed.clear())
    rawJobs.clear()
    errors.clear()
  }

  // ---------------------------------------------------------------- listener-facing API

  def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    executionStarts.put(event.executionId, event)
    attributionOf(event).foreach(id => executionEntry.put(event.executionId, id))
  }

  def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val start = Option(executionStarts.remove(event.executionId))
    val entryId = Option(executionEntry.get(event.executionId))
    val funcName = SqlEventAccess.executionNameOf(event)
    val failure =
      SqlEventAccess.failureOf(event).map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")
    var readsDeltaLog = false
    val planReport = SqlEventAccess.qeOf(event).flatMap {
      qe =>
        try {
          val executedPlan = qe.executedPlan
          readsDeltaLog = PlanCollector.plansOverDeltaLog(executedPlan)
          val report = classifier.classify(executedPlan)
          Some(enrichReasons(report, event.executionId))
        } catch {
          case NonFatal(t) =>
            entryId.foreach(
              id => addError(id, t.getClass.getSimpleName, s"classifier threw: ${t.getMessage}"))
            None
        }
    }
    val isNested = start.exists(s => s.rootExecutionId.exists(_ != event.executionId))
    val phase = determinePhase(funcName, isNested, readsDeltaLog)
    val record = ExecutionRecord(
      executionId = event.executionId,
      entryId = entryId,
      phase = phase,
      funcName = funcName,
      description = start.map(_.description).getOrElse("").take(200),
      planReport = planReport,
      durationMs = SqlEventAccess.durationNsOf(event) / 1000000L,
      error = failure
    )
    entryId match {
      case Some(id) =>
        val buf = records.computeIfAbsent(id, _ => new ArrayBuffer[ExecutionRecord]())
        buf.synchronized(buf += record)
      case None =>
        unattributed.synchronized(unattributed += record)
    }
  }

  def onGlutenPlanFallback(executionId: Long, fallbackNodeToReason: Map[String, String]): Unit = {
    // Strip the "NNN " operator-id prefix from GlutenExplainUtils keys, keep first reason per node.
    val byNodeName = fallbackNodeToReason.map {
      case (key, reason) => (key.replaceFirst("^\\d+\\s+", ""), reason)
    }
    glutenReasons.merge(executionId, byNodeName, (a, b) => a ++ b)
  }

  /** Raw jobs with no SQL execution (e.g. MERGE source materialization). */
  def onJobStartWithoutExecution(properties: Properties): Unit = {
    val tags = Option(properties.getProperty(JOB_TAGS_PROP))
      .map(_.split(",").toSet)
      .getOrElse(Set.empty[String])
    val entry = tags
      .collectFirst { case t if t.startsWith(TAG_PREFIX) => t.stripPrefix(TAG_PREFIX) }
      .orElse(window)
    entry.foreach(id => rawJobs.merge(id, 1, (a, b) => a + b))
  }

  // ---------------------------------------------------------------- internals

  private def attributionOf(event: SparkListenerSQLExecutionStart): Option[String] = {
    val fromTag = event.jobTags.collectFirst {
      case t if t.startsWith(TAG_PREFIX) => t.stripPrefix(TAG_PREFIX)
    }
    val fromRoot = event.rootExecutionId
      .filter(_ != event.executionId)
      .flatMap(root => Option(executionEntry.get(root)))
    fromTag.orElse(fromRoot).orElse(window)
  }

  /**
   * Backfills reasons from GlutenPlanFallbackEvent for fallback nodes that had no recoverable
   * FallbackTag. The Gluten event is posted during planning, before the end event, and the listener
   * bus preserves per-queue ordering, so the reasons are present by end-event time.
   */
  private def enrichReasons(report: PlanReport, executionId: Long): PlanReport = {
    val reasons = Option(glutenReasons.get(executionId)).getOrElse(Map.empty)
    if (reasons.isEmpty) return report
    PlanReport(report.nodes.map {
      case n @ ClassifiedNode(
            _,
            nodeName,
            _,
            _,
            NodeVerdict.Fallback(op, Classifier.NO_REASON_RECORDED)) =>
        reasons.get(nodeName) match {
          case Some(reason) => n.copy(verdict = NodeVerdict.Fallback(op, reason))
          case None => n
        }
      case n => n
    })
  }

  private def determinePhase(
      funcName: Option[String],
      isNested: Boolean,
      readsDeltaLog: Boolean): ExecutionPhase = {
    funcName match {
      case Some(DELTA_WRITE_FUNC) => ExecutionPhase.Write
      case Some(DELTA_CHECKPOINT_FUNC) => ExecutionPhase.DeltaMetadata
      case Some(n) if n.startsWith(STATE_CACHE_FUNC_PREFIX) => ExecutionPhase.DeltaMetadata
      case Some(n) if COMMAND_FUNC_NAMES.contains(n) => ExecutionPhase.CommandWrapper
      case _ if readsDeltaLog => ExecutionPhase.DeltaMetadata
      case _ if isNested => ExecutionPhase.DmlScan
      case _ => ExecutionPhase.Query
    }
  }
}

object PlanCollector {
  val TAG_PREFIX: String = "gluten-coverage:"
  val JOB_TAGS_PROP: String = "spark.job.tags"

  private val DELTA_WRITE_FUNC = "deltaTransactionalWrite"
  private val DELTA_CHECKPOINT_FUNC = "Delta checkpoint"
  private val STATE_CACHE_FUNC_PREFIX = "Cache "

  /**
   * Execution names Spark uses for eagerly-executed commands (QueryExecution.commandExecutionName).
   */
  private val COMMAND_FUNC_NAMES =
    Set("command", "create", "replace", "append", "overwrite", "overwritePartitions")

  /** Detects executions whose scans read the Delta transaction log. */
  def plansOverDeltaLog(plan: SparkPlan): Boolean = {
    plan.collectFirst {
      case f: FileSourceScanExec
          if f.relation.location.rootPaths.exists(_.toString.contains("_delta_log")) =>
        f
    }.isDefined
  }
}
