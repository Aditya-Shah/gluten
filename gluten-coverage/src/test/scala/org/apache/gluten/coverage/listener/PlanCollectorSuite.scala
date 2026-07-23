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

import org.apache.gluten.coverage.NodeVerdict

import org.apache.spark.CoverageSparkHooks
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.coverage.SqlEventAccess
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/**
 * Integration tests for attribution and capture against a real local SparkSession (vanilla Spark:
 * Gluten classes are on the classpath but the plugin is not active, so plans classify via the
 * CounterpartMap defensive branch -- enough to verify capture, attribution, phases, and AQE
 * unwrapping).
 */
class PlanCollectorSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var collector: PlanCollector = _
  private var listener: CoverageSparkListener = _

  override def beforeAll(): Unit = {
    val warehouse = Files.createTempDirectory("coverage-warehouse").toFile.getAbsolutePath
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("gluten-coverage-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()
    collector = new PlanCollector()
    listener = new CoverageSparkListener(collector)
    spark.sparkContext.addSparkListener(listener)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.removeSparkListener(listener)
      spark.stop()
    }
  }

  private def drain(): Unit =
    CoverageSparkHooks.waitUntilListenerBusEmpty(spark.sparkContext, 30000)

  test("window attribution captures a query execution and unwraps AQE") {
    collector.clear()
    collector.openWindow("agg-entry")
    try {
      spark.sql("SELECT id % 3 AS g, count(*) FROM range(0, 100) GROUP BY 1").collect()
    } finally {
      drain()
      collector.closeWindow()
    }
    val records = collector.recordsFor("agg-entry")
    assert(records.size == 1, s"expected 1 execution, got: $records")
    val record = records.head
    assert(record.phase == ExecutionPhase.Query)
    val report = record.planReport.get
    // AQE wraps shuffle queries; the classifier must descend into the materialized stages
    // instead of reporting a single AdaptiveSparkPlanExec node.
    assert(report.nodes.exists(_.opClass == "AdaptiveSparkPlanExec"))
    assert(
      report.nodes
        .find(_.opClass == "AdaptiveSparkPlanExec")
        .get
        .verdict
        .isInstanceOf[NodeVerdict.Neutral])
    assert(report.nodes.exists(_.opClass == "HashAggregateExec"), report.nodes.map(_.opClass))
    assert(report.nodes.count(_.opClass == "ShuffleQueryStageExec") >= 1)
  }

  test("job-tag attribution works without a window") {
    collector.clear()
    val tag = s"${PlanCollector.TAG_PREFIX}tagged-entry"
    spark.sparkContext.addJobTag(tag)
    try {
      spark.sql("SELECT id FROM range(0, 10) WHERE id > 5").collect()
    } finally {
      drain()
      spark.sparkContext.removeJobTag(tag)
    }
    val records = collector.recordsFor("tagged-entry")
    assert(records.size == 1)
    assert(records.head.entryId.contains("tagged-entry"))
  }

  test("executions with no tag, window, or root land in the unattributed bucket") {
    collector.clear()
    spark.sql("SELECT 1").collect()
    drain()
    assert(collector.recordsFor("nope").isEmpty)
    assert(collector.unattributedRecords.nonEmpty)
  }

  test("eager command execution (CTAS) is captured with the command phase") {
    collector.clear()
    collector.openWindow("ctas-entry")
    try {
      spark.sql("CREATE TABLE cov_ctas USING parquet AS SELECT id FROM range(0, 10)").collect()
    } finally {
      drain()
      collector.closeWindow()
    }
    try {
      val records = collector.recordsFor("ctas-entry")
      assert(records.nonEmpty, "CTAS produced no captured executions")
      // The eager command execution happens inside spark.sql() itself -- the exact miss of the
      // old tag-after-sql() approach. Its funcName is one of Spark's command execution names.
      val command = records.find(_.phase == ExecutionPhase.CommandWrapper)
      assert(command.isDefined, s"no command-phase execution among: ${records.map(_.funcName)}")
      val wrapperNodes = command.get.planReport.get.nodes
      assert(wrapperNodes.exists(_.verdict.isInstanceOf[NodeVerdict.Neutral]))
    } finally {
      spark.sql("DROP TABLE IF EXISTS cov_ctas")
      drain()
    }
  }

  test("root-execution-id chaining inherits attribution from the parent execution") {
    collector.clear()
    val qe = spark.range(5).queryExecution
    val info = new SparkPlanInfo("Range", "Range", Seq.empty, Map.empty, Seq.empty)

    val parentStart = SparkListenerSQLExecutionStart(
      executionId = 900L,
      rootExecutionId = Some(900L),
      description = "parent",
      details = "",
      physicalPlanDescription = "",
      sparkPlanInfo = info,
      time = 0L,
      modifiedConfigs = Map.empty,
      jobTags = Set(s"${PlanCollector.TAG_PREFIX}root-entry")
    )
    val childStart = SparkListenerSQLExecutionStart(
      executionId = 901L,
      rootExecutionId = Some(900L),
      description = "child",
      details = "",
      physicalPlanDescription = "",
      sparkPlanInfo = info,
      time = 0L,
      modifiedConfigs = Map.empty,
      jobTags = Set.empty
    )
    collector.onExecutionStart(parentStart)
    collector.onExecutionStart(childStart)

    val childEnd = SparkListenerSQLExecutionEnd(901L, 1L, Some(""))
    SqlEventAccess.setQe(childEnd, qe)
    SqlEventAccess.setExecutionName(childEnd, None)
    SqlEventAccess.setDurationNs(childEnd, 5000000L)
    collector.onExecutionEnd(childEnd)

    val records = collector.recordsFor("root-entry")
    assert(records.size == 1)
    assert(records.head.executionId == 901L)
    // Nested + unnamed => data-path scan phase.
    assert(records.head.phase == ExecutionPhase.DmlScan)
  }

  test("delta funcNames map to write and metadata phases") {
    collector.clear()
    val qe = spark.range(5).queryExecution
    val info = new SparkPlanInfo("Range", "Range", Seq.empty, Map.empty, Seq.empty)

    def run(execId: Long, funcName: String): Unit = {
      val start = SparkListenerSQLExecutionStart(
        executionId = execId,
        rootExecutionId = Some(execId),
        description = funcName,
        details = "",
        physicalPlanDescription = "",
        sparkPlanInfo = info,
        time = 0L,
        modifiedConfigs = Map.empty,
        jobTags = Set(s"${PlanCollector.TAG_PREFIX}phase-entry")
      )
      collector.onExecutionStart(start)
      val end = SparkListenerSQLExecutionEnd(execId, 1L, Some(""))
      SqlEventAccess.setQe(end, qe)
      SqlEventAccess.setExecutionName(end, Some(funcName))
      collector.onExecutionEnd(end)
    }

    run(910L, "deltaTransactionalWrite")
    run(911L, "Delta checkpoint")
    run(912L, "Cache Delta Table State #1 - /tmp/t/_delta_log")

    val phases = collector.recordsFor("phase-entry").map(r => r.funcName.get -> r.phase).toMap
    assert(phases("deltaTransactionalWrite") == ExecutionPhase.Write)
    assert(phases("Delta checkpoint") == ExecutionPhase.DeltaMetadata)
    assert(
      phases("Cache Delta Table State #1 - /tmp/t/_delta_log") ==
        ExecutionPhase.DeltaMetadata)
  }

  test("GlutenPlanFallbackEvent reasons enrich no-reason fallback nodes by node name") {
    collector.clear()
    val qe = spark.range(10).filter("id > 1").queryExecution
    val info = new SparkPlanInfo("Filter", "Filter", Seq.empty, Map.empty, Seq.empty)

    val start = SparkListenerSQLExecutionStart(
      executionId = 920L,
      rootExecutionId = Some(920L),
      description = "filter",
      details = "",
      physicalPlanDescription = "",
      sparkPlanInfo = info,
      time = 0L,
      modifiedConfigs = Map.empty,
      jobTags = Set(s"${PlanCollector.TAG_PREFIX}gluten-entry")
    )
    collector.onExecutionStart(start)
    collector.onGlutenPlanFallback(920L, Map("001 Filter" -> "native says no thanks"))
    val end = SparkListenerSQLExecutionEnd(920L, 1L, Some(""))
    SqlEventAccess.setQe(end, qe)
    SqlEventAccess.setExecutionName(end, Some("collect"))
    collector.onExecutionEnd(end)

    val report = collector.recordsFor("gluten-entry").head.planReport.get
    val filterNode = report.nodes.find(_.opClass == "FilterExec").get
    assert(filterNode.verdict == NodeVerdict.Fallback("FilterExec", "native says no thanks"))
  }

  test("raw jobs without a SQL execution are counted per entry") {
    collector.clear()
    collector.openWindow("raw-entry")
    try {
      // RDD action with no SQL execution -- the MERGE-materialization shape.
      spark.sparkContext.parallelize(1 to 10, 2).count()
    } finally {
      drain()
      collector.closeWindow()
    }
    assert(collector.rawJobCount("raw-entry") == 1)
    assert(collector.recordsFor("raw-entry").isEmpty)
  }
}
