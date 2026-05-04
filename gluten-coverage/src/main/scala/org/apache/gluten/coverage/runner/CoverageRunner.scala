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
package org.apache.gluten.coverage.runner

import org.apache.gluten.coverage.emitter.{BaselineDiffer, JsonEmitter, MarkdownEmitter}
import org.apache.gluten.coverage.listener.{CoverageQueryExecutionListener, CoverageTags, PlanCollector}
import org.apache.gluten.coverage.matrix.{BuildEnvironment, LoadedMatrix, MatrixEntry, MatrixLoader}
import org.apache.gluten.coverage.report.{CoverageReport, ReportBuilder}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Standalone driver for the coverage tool. Loads matrices via ServiceLoader, runs each entry's
 * setup/query/teardown SQL against a single SparkSession, captures executed plans via a
 * QueryExecutionListener, and writes JSON + Markdown reports.
 *
 * Invoked as `java -cp ... org.apache.gluten.coverage.runner.CoverageRunner [options]`.
 */
object CoverageRunner {

  // scalastyle:off println

  def main(args: Array[String]): Unit = {
    val opts = RunnerOptions.parse(args)
    val exitCode = runWith(opts)
    sys.exit(exitCode)
  }

  def runWith(opts: RunnerOptions): Int = {
    val spark = newSparkSession()
    try {
      val env = buildEnvironment(spark, opts)
      val loader = new MatrixLoader(env)
      val matrices =
        loader.loadAll().filter(m => opts.matrixId.forall(id => m.descriptor.id == id))
      if (matrices.isEmpty) {
        opts.matrixId match {
          case Some(id) => System.err.println(s"No matrix found with id='$id'.")
          case None => System.err.println("No matrices registered via ServiceLoader.")
        }
        return 2
      }

      val collector = new PlanCollector()
      val listener = new CoverageQueryExecutionListener(collector)
      spark.listenerManager.register(listener)
      try {
        matrices.foreach {
          matrix =>
            val report = runMatrix(spark, matrix, collector, env, opts)
            writeOutputs(report, opts)
        }
      } finally {
        spark.listenerManager.unregister(listener)
      }
      0
    } finally {
      spark.stop()
    }
  }

  private def runMatrix(
      spark: SparkSession,
      matrix: LoadedMatrix,
      collector: PlanCollector,
      env: BuildEnvironment,
      opts: RunnerOptions): CoverageReport = {

    println(
      s"[gluten-coverage] running matrix '${matrix.descriptor.id}' " +
        s"(${matrix.entries.size} entries) ...")

    val outcomes = matrix.entries.map {
      entry =>
        val versionSkipped =
          !env.supportsDelta(entry.deltaMinVersion) || !env.supportsSpark(entry.sparkMinVersion)
        if (versionSkipped) {
          println(s"  [skip] ${entry.id} (version-gated)")
        } else {
          runEntry(spark, entry)
        }
        ReportBuilder.buildOutcome(entry, collector, env, versionSkipped)
    }

    val report = ReportBuilder.build(matrix, outcomes, env, includePlanTree = opts.verbose)
    val gateCheck = opts.baseline.map {
      f =>
        val baseline = JsonEmitter.parseFile(f)
        BaselineDiffer.diff(report, Some(baseline))
    }
    val finalReport = report.copy(gateCheck = gateCheck)

    println(
      s"[gluten-coverage] matrix '${matrix.descriptor.id}' done: " +
        s"entry-pure-native=${finalReport.summary.entryPureNativePercent}% " +
        s"node-weighted=${finalReport.summary.nodeWeightedPercent}%")

    finalReport
  }

  private def runEntry(spark: SparkSession, entry: MatrixEntry): Unit = {
    val priorConfig = applyConfigOverrides(spark, entry.config)
    try {
      entry.setupSql.foreach(sql => runUntagged(spark, sql))
      entry.querySql.foreach(sql => runTagged(spark, sql, entry.id))
      entry.teardownSql.foreach(sql => runUntagged(spark, sql))
    } catch {
      case t: Throwable =>
        // Record as error; we attribute to entry.id since the listener may not have fired.
        // The collector merges any plans the listener did capture before the throw.
        System.err.println(s"  [error] ${entry.id}: ${t.getClass.getSimpleName}: ${t.getMessage}")
    } finally {
      restoreConfig(spark, priorConfig)
    }
  }

  /**
   * Runs a SQL string, splitting on semicolons so multi-statement setup/teardown blocks work. No
   * tag is set; the listener will ignore.
   */
  private def runUntagged(spark: SparkSession, sql: String): Unit = {
    splitStatements(sql).foreach {
      stmt =>
        val df = spark.sql(stmt)
        df.collect()
    }
  }

  /**
   * Tags the parsed logical plan with the entry id so the listener can attribute the executed plan
   * back to this matrix entry. The first statement in a multi-statement block is the tagged one
   * (the matrix convention is that `query_sql` is one statement).
   */
  private def runTagged(spark: SparkSession, sql: String, entryId: String): Unit = {
    val statements = splitStatements(sql)
    statements.zipWithIndex.foreach {
      case (stmt, idx) =>
        val df = spark.sql(stmt)
        if (idx == 0) {
          df.queryExecution.logical.setTagValue(CoverageTags.ENTRY_ID, entryId)
        }
        df.collect()
    }
  }

  private def splitStatements(sql: String): Seq[String] = sql
    .split(';')
    .map(_.trim)
    .filter(_.nonEmpty)
    .toSeq

  /**
   * Applies the entry's config overrides via `spark.conf.set` and returns the prior values so they
   * can be restored afterwards.
   */
  private def applyConfigOverrides(
      spark: SparkSession,
      configOpt: Option[Map[String, String]]): Map[String, Option[String]] = {
    configOpt match {
      case None => Map.empty
      case Some(cfg) =>
        cfg.map {
          case (k, v) =>
            val prev =
              try Some(spark.conf.get(k))
              catch { case _: java.util.NoSuchElementException => None }
            spark.conf.set(k, v)
            k -> prev
        }
    }
  }

  private def restoreConfig(spark: SparkSession, prior: Map[String, Option[String]]): Unit = {
    prior.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None) => spark.conf.unset(k)
    }
  }

  private def writeOutputs(report: CoverageReport, opts: RunnerOptions): Unit = {
    JsonEmitter.emitTo(report, opts.outputJson)
    println(s"[gluten-coverage] wrote ${opts.outputJson.getAbsolutePath}")

    val md = new MarkdownEmitter(opts.verbose)
    md.emitTo(report, opts.outputMarkdown)
    println(s"[gluten-coverage] wrote ${opts.outputMarkdown.getAbsolutePath}")
  }

  private def newSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("gluten-coverage")
      .master(sys.props.getOrElse("spark.master", "local[2]"))
      .getOrCreate()
  }

  private def buildEnvironment(spark: SparkSession, opts: RunnerOptions): BuildEnvironment = {
    BuildEnvironment(
      sparkVersion = SparkContext.getOrCreate().version,
      deltaVersion =
        opts.deltaVersionOverride.getOrElse(sys.props.getOrElse("delta.version", "unknown")),
      glutenVersion =
        opts.glutenVersionOverride.getOrElse(sys.props.getOrElse("gluten.version", "unknown")),
      scalaBinaryVersion =
        scala.util.Properties.versionNumberString.split('.').take(2).mkString("."),
      jdkVersion = sys.props.getOrElse("java.specification.version", "unknown"),
      backend = opts.backendOverride.getOrElse("velox")
    )
  }

  // scalastyle:on println
}
