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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.extension.columnar.transition.Transitions

import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.files.{GlutenDeltaFileFormatWriter, TransactionalWrite}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.perf.{DeltaOptimizedWriterExec, GlutenDeltaOptimizedWriterExec}
import org.apache.spark.sql.delta.schema.InnerInvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.GlutenDeltaJobStatisticsTracker
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ListBuffer

class GlutenOptimisticTransaction(delegate: OptimisticTransaction)
  extends OptimisticTransaction(
    delegate.deltaLog,
    delegate.catalogTable,
    delegate.snapshot
  ) {

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession

    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _trackFromData) =
      normalizeData(deltaLog, writeOptions, data)
    // Delta 3.2.1: normalizeData returns the trackHighWaterMarks set directly; the Option-wrapped
    // OptimisticTransaction.trackHighWaterMarks field lands in 3.3. IDENTITY-column tracking is
    // intentionally dropped on this overlay (see usage strip-out below).

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    val (statsDataSchema, _) = getStatsSchema(output, partitionSchema)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val optionalStatsTracker =
      getOptionalStatsTrackerAndStatsCollection(output, outputPath, partitionSchema, data)._1.map(
        new GlutenDeltaJobStatisticsTracker(_))

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    // IDENTITY-column high-water-mark tracking is not wired on the Delta 3.2.1 overlay
    // (IdentityColumn.createIdentityColumnStatsTracker, DeltaIdentityColumnStatsTracker,
    // OptimisticTransaction.updatedIdentityHighWaterMarks all land in 3.3). Tables that
    // use IDENTITY columns through this code path will not advance high-water marks on
    // Gluten-driven writes.

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val empty2NullPlan =
        convertEmptyToNullIfNeeded(queryExecution.executedPlan, partitioningColumns, constraints)
      val maybeCheckInvariants = if (constraints.isEmpty) {
        // Compared to vanilla Delta, we simply avoid adding the invariant checker
        // when the constraint list is empty, to omit the unnecessary transitions
        // added around the invariant checker.
        empty2NullPlan
      } else {
        DeltaInvariantCheckerExec(empty2NullPlan, constraints)
      }
      def toVeloxPlan(plan: SparkPlan): SparkPlan = plan match {
        case aqe: AdaptiveSparkPlanExec =>
          assert(!aqe.isFinalPlan)
          aqe.copy(supportsColumnar = true)
        case _ => Transitions.toBatchPlan(maybeCheckInvariants, VeloxBatchType)
      }
      // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
      // evenly-balanced data files already.
      val physicalPlan =
        if (
          !isOptimize &&
          shouldOptimizeWrite(writeOptions, spark.sessionState.conf)
        ) {
          // We uniformly convert the query plan to a columnar plan. If
          // the further write operation turns out to be non-offload-able, the
          // columnar plan will be converted back to a row-based plan.
          val veloxPlan = toVeloxPlan(maybeCheckInvariants)
          try {
            val glutenWriterExec =
              GlutenDeltaOptimizedWriterExec(veloxPlan, metadata.partitionColumns, deltaLog)
            val validationResult = glutenWriterExec.doValidate()
            if (validationResult.ok()) {
              glutenWriterExec
            } else {
              logInfo(
                s"GlutenDeltaOptimizedWriterExec: Internal shuffle validated negative," +
                  s" reason: ${validationResult.reason()}. Falling back to row-based shuffle.")
              DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
            }
          } catch {
            case e: AnalysisException =>
              logWarning(
                s"GlutenDeltaOptimizedWriterExec: Failed to create internal shuffle," +
                  s" reason: ${e.getMessage()}. Falling back to row-based shuffle.")
              DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
          }
        } else {
          val veloxPlan = toVeloxPlan(maybeCheckInvariants)
          veloxPlan
        }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Iceberg spec requires partition columns in data files
      val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata)
      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = (writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filterKeys {
            key =>
              key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }.toMap
      }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)

      try {
        GlutenDeltaFileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = new GlutenDeltaParquetFileFormat(
            protocol,
            metadata
          ), // This is changed to Gluten's Delta format.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
          options = options
        )
      } catch {
        case InnerInvariantViolationException(violationException) =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          throw violationException
      }
      // IdentityColumn.logTableWrite is 3.3+; no-op on the 3.2.1 overlay.
    }

    var resultFiles =
      (if (optionalStatsTracker.isDefined) {
         committer.addedStatuses.map {
           a =>
             a.copy(stats = optionalStatsTracker
               .map(_.delegate.recordedStats(a.toPath.getName))
               .getOrElse(a.stats))
         }
       } else {
         committer.addedStatuses
       })
        .filter {
          // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
          // may be fixed in the future) are the MERGE command when you delete with empty source, or
          // empty target, or on disjoint tables. This is hard to catch before the write without
          // collecting the DF ahead of time. Instead, we can return only the AddFiles that
          // a) actually add rows, or
          // b) don't have any stats so we don't know the number of rows at all
          case a: AddFile => a.numLogicalRecords.forall(_ > 0)
          case _ => true
        }

    // add [[AddFile.Tags.ICEBERG_COMPAT_VERSION.name]] tags to addFiles
    if (IcebergCompatV2.isEnabled(metadata)) {
      resultFiles = resultFiles.map {
        addFile =>
          val tags = if (addFile.tags != null) addFile.tags else Map.empty[String, String]
          addFile.copy(tags = tags + (AddFile.Tags.ICEBERG_COMPAT_VERSION.name -> "2"))
      }
    }

    if (resultFiles.nonEmpty && !isOptimize) registerPostCommitHook(AutoCompact)
    // OptimisticTransaction.updatedIdentityHighWaterMarks is 3.3+; nothing to record on 3.2.1.

    resultFiles.toSeq ++ committer.changeFiles
  }

  private def shouldOptimizeWrite(
      writeOptions: Option[DeltaOptions],
      sessionConf: SQLConf): Boolean = {
    writeOptions
      .flatMap(_.optimizeWrite)
      .getOrElse(TransactionalWrite.shouldOptimizeWrite(metadata, sessionConf))
  }
}
