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
package org.apache.spark.sql.coverage

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Shim into the `private[sql]` fields of [[SparkListenerSQLExecutionEnd]]. Spark attaches the live
 * `QueryExecution` (`event.qe`) to every execution-end event -- named or not -- which is what lets
 * a `SparkListener` see Delta's nested command executions that a `QueryExecutionListener` never
 * receives. The setters exist for test fixtures only.
 */
object SqlEventAccess {

  def qeOf(event: SparkListenerSQLExecutionEnd): Option[QueryExecution] = Option(event.qe)

  def executionNameOf(event: SparkListenerSQLExecutionEnd): Option[String] = event.executionName

  def durationNsOf(event: SparkListenerSQLExecutionEnd): Long = event.duration

  def failureOf(event: SparkListenerSQLExecutionEnd): Option[Throwable] = event.executionFailure

  // Test-fixture setters (events are constructed synthetically in unit tests).
  def setQe(event: SparkListenerSQLExecutionEnd, qe: QueryExecution): Unit = event.qe = qe

  def setExecutionName(event: SparkListenerSQLExecutionEnd, name: Option[String]): Unit =
    event.executionName = name

  def setDurationNs(event: SparkListenerSQLExecutionEnd, durationNs: Long): Unit =
    event.duration = durationNs
}
