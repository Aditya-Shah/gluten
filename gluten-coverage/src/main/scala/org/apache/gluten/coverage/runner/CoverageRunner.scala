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
import org.apache.gluten.coverage.listener.{CoverageSparkListener, PlanCollector}
import org.apache.gluten.coverage.matrix.{BuildEnvironment, LoadedMatrix, MatrixEntry, MatrixLoader}
import org.apache.gluten.coverage.report.{CoverageReport, ReportBuilder}

import org.apache.spark.{CoverageSparkHooks, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Standalone driver for the coverage tool. Loads matrices via ServiceLoader, runs each entry's
 * setup/query/teardown SQL against a single SparkSession, captures every SQL execution (including
 * nested executions spawned by commands) via a SparkListener, and writes JSON + Markdown reports.
 *
 * Attribution: each entry's `query_sql` runs inside a bracket of `sc.addJobTag` + an open collector
 * window, with the listener bus drained at the edges so events cannot bleed between entries. See
 * [[PlanCollector]] for the attribution layers.
 *
 * Invoked as `java -cp ... org.apache.gluten.coverage.runner.CoverageRunner [options]`.
 */
object CoverageRunner {

  // scalastyle:off println

  private val LISTENER_BUS_DRAIN_TIMEOUT_MS = 60000L

  def main(args: Array[String]): Unit = {
    val opts = RunnerOptions.parse(args)
    val exitCode = runWith(opts)
    sys.exit(exitCode)
  }

  def runWith(opts: RunnerOptions): Int = {
    val spark = newSparkSession()
    try {
      warnIfFallbackReportingDisabled(spark)
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
      val listener = new CoverageSparkListener(collector)
      spark.sparkContext.addSparkListener(listener)
      try {
        matrices.foreach {
          matrix =>
            val report = runMatrix(spark, matrix, collector, env, opts)
            writeOutputs(report, opts)
        }
      } finally {
        spark.sparkContext.removeSparkListener(listener)
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

    collector.clear()
    val outcomes = matrix.entries.map {
      entry =>
        val versionSkipped =
          !env.supportsDelta(entry.deltaMinVersion) || !env.supportsSpark(entry.sparkMinVersion)
        if (versionSkipped) {
          println(s"  [skip] ${entry.id} (version-gated)")
        } else {
          runEntry(spark, entry, collector)
        }
        ReportBuilder.buildOutcome(entry, collector, versionSkipped)
    }

    val report = ReportBuilder.build(
      matrix,
      outcomes,
      env,
      includePlanTree = opts.verbose,
      metadataWeight = opts.metadataWeight,
      unattributedExecutions = collector.unattributedRecords.size
    )
    val gateCheck = opts.baseline.map {
      f =>
        val baseline = JsonEmitter.parseFile(f)
        require(
          baseline.schemaVersion == ReportBuilder.SCHEMA_VERSION,
          s"Baseline ${f.getPath} has schema_version=${baseline.schemaVersion}; this tool " +
            s"writes schema_version=${ReportBuilder.SCHEMA_VERSION}. Regenerate the baseline " +
            "with this tool version before gating."
        )
        BaselineDiffer.diff(report, Some(baseline))
    }
    val finalReport = report.copy(gateCheck = gateCheck)

    println(
      s"[gluten-coverage] matrix '${matrix.descriptor.id}' done: " +
        s"entry-pure-native=${finalReport.summary.entryPureNativePercent}% " +
        s"node-weighted=${finalReport.summary.nodeWeightedPercent}% " +
        s"multi-execution-entries=${finalReport.summary.entriesMultiExecution}")

    finalReport
  }

  private def runEntry(spark: SparkSession, entry: MatrixEntry, collector: PlanCollector): Unit = {
    val sc = spark.sparkContext
    val tag = s"${PlanCollector.TAG_PREFIX}${entry.id}"
    val priorConfig = applyConfigOverrides(spark, entry.config)
    try {
      entry.setupSql.foreach(sql => runStatements(spark, sql))
      drainListenerBus(sc)
      sc.addJobTag(tag)
      collector.openWindow(entry.id)
      try {
        entry.querySql.foreach(sql => runStatements(spark, sql))
      } finally {
        drainListenerBus(sc)
        collector.closeWindow()
        sc.removeJobTag(tag)
      }
      entry.teardownSql.foreach(sql => runStatements(spark, sql))
      drainListenerBus(sc)
    } catch {
      case t: Throwable =>
        // Record as error; plans the listener captured before the throw are kept and merged.
        System.err.println(s"  [error] ${entry.id}: ${t.getClass.getSimpleName}: ${t.getMessage}")
        collector.addError(entry.id, t.getClass.getSimpleName, String.valueOf(t.getMessage))
        // The window may still be open if the failure happened in setup; make sure it is not.
        collector.closeWindow()
        sc.removeJobTag(tag)
    } finally {
      restoreConfig(spark, priorConfig)
    }
  }

  /** Runs a SQL string, splitting on semicolons so multi-statement blocks work. */
  private def runStatements(spark: SparkSession, sql: String): Unit = {
    splitStatements(sql).foreach {
      stmt =>
        val df = spark.sql(stmt)
        df.collect()
    }
  }

  private def drainListenerBus(sc: SparkContext): Unit = {
    CoverageSparkHooks.waitUntilListenerBusEmpty(sc, LISTENER_BUS_DRAIN_TIMEOUT_MS)
  }

  private def splitStatements(sql: String): Seq[String] = sql
    .split(';')
    .map(_.trim)
    .filter(_.nonEmpty)
    .toSeq

  private def warnIfFallbackReportingDisabled(spark: SparkSession): Unit = {
    val reporter =
      spark.conf.getOption("spark.gluten.sql.columnar.fallbackReporter").getOrElse("true")
    val ui = spark.conf.getOption("spark.gluten.ui.enabled").getOrElse("true")
    if (reporter != "true" || ui != "true") {
      System.err.println(
        "[gluten-coverage] WARNING: spark.gluten.sql.columnar.fallbackReporter and " +
          "spark.gluten.ui.enabled should both be true; without them fallback reasons degrade " +
          s"to '${org.apache.gluten.coverage.classifier.Classifier.NO_REASON_RECORDED}'.")
    }
  }

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
