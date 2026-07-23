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

import org.apache.gluten.coverage.PlanReport

/**
 * The role a captured SQL execution plays within a matrix entry. A command entry (UPDATE, MERGE,
 * OPTIMIZE, ...) produces several executions; the phase determines whether an execution's nodes
 * count toward the entry's verdict and how the report buckets it.
 */
sealed trait ExecutionPhase {
  def name: String

  /** Whether nodes of this phase feed the entry verdict and the data-path (headline) metrics. */
  def countedInVerdict: Boolean
}

object ExecutionPhase {

  /**
   * The eager command execution itself (funcName `command`/`create`/...). For Delta V1 commands its
   * plan is an inert `ExecutedCommandExec` wrapper (all Neutral); for Iceberg V2 commands the full
   * scan+write plan lives here, so the phase is counted.
   */
  case object CommandWrapper extends ExecutionPhase {
    val name = "command"
    val countedInVerdict = true
  }

  /** A Delta transactional write (`deltaTransactionalWrite`) or V2 write execution. */
  case object Write extends ExecutionPhase {
    val name = "write"
    val countedInVerdict = true
  }

  /** A nested Dataset action inside a command (findTouchedFiles collect, DV bitmap build, ...). */
  case object DmlScan extends ExecutionPhase {
    val name = "dml-scan"
    val countedInVerdict = true
  }

  /** A plain top-level query execution (SELECT-shaped entries). */
  case object Query extends ExecutionPhase {
    val name = "query"
    val countedInVerdict = true
  }

  /**
   * Delta-log bookkeeping: snapshot state reconstruction (`Cache Delta Table State ...`),
   * checkpoint writes (`Delta checkpoint`), scans over `_delta_log/`. Always captured and reported,
   * weighted at `--metadata-weight` in the node-weighted metric, but excluded from the entry
   * verdict so log replay does not mark every command entry as fallback.
   */
  case object DeltaMetadata extends ExecutionPhase {
    val name = "delta-metadata"
    val countedInVerdict = false
  }

  /**
   * A raw Spark job with no surrounding SQL execution (e.g. MERGE source materialization's
   * RDD.foreach). There is no plan to classify; recorded so the report can state how many raw jobs
   * ran instead of silently dropping them.
   */
  case object JobOnly extends ExecutionPhase {
    val name = "job-only"
    val countedInVerdict = false
  }

  def fromName(name: String): ExecutionPhase = name match {
    case CommandWrapper.name => CommandWrapper
    case Write.name => Write
    case DmlScan.name => DmlScan
    case Query.name => Query
    case DeltaMetadata.name => DeltaMetadata
    case JobOnly.name => JobOnly
    case other => throw new IllegalArgumentException(s"Unknown execution phase: $other")
  }
}

/**
 * One captured SQL execution (or raw job), attributed to a matrix entry (or unattributed when no
 * attribution layer matched). Immutable; assembled by the [[PlanCollector]] on the listener-bus
 * thread at execution-end time so no live `QueryExecution` reference is retained.
 */
case class ExecutionRecord(
    executionId: Long,
    entryId: Option[String],
    phase: ExecutionPhase,
    funcName: Option[String],
    description: String,
    planReport: Option[PlanReport],
    durationMs: Long,
    error: Option[String])
