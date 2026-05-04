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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Thread-safe accumulator of (entryId -> classified plan reports). One matrix entry may produce
 * multiple plans (e.g. a MERGE that decomposes into multiple Spark executions); the collector
 * merges them so the per-entry totals are over the union.
 *
 * Errors and durations are tracked alongside the plan reports.
 */
class PlanCollector {

  private val plans = new ConcurrentHashMap[String, PlanReport]()
  private val durations = new ConcurrentHashMap[String, Long]()
  private val errors = new ConcurrentHashMap[String, String]()

  def addPlan(entryId: String, report: PlanReport, durationNs: Long): Unit = {
    plans.merge(entryId, report, (a, b) => PlanReport.merge(a, b))
    durations.merge(entryId, durationNs, (a, b) => a + b)
  }

  def addError(entryId: String, errorClass: String, message: String): Unit = {
    val combined = s"$errorClass: $message"
    errors.put(entryId, combined)
  }

  def getPlan(entryId: String): Option[PlanReport] = Option(plans.get(entryId))

  def getDurationNs(entryId: String): Long = durations.getOrDefault(entryId, 0L)

  def getError(entryId: String): Option[String] = Option(errors.get(entryId))

  def hasPlan(entryId: String): Boolean = plans.containsKey(entryId)

  def hasError(entryId: String): Boolean = errors.containsKey(entryId)

  def allEntryIds: Set[String] =
    (plans.keySet().asScala.toSet ++ errors.keySet().asScala.toSet)

  def clear(): Unit = {
    plans.clear()
    durations.clear()
    errors.clear()
  }
}
