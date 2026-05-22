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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.{DeltaExcludedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.hadoop.fs.Path

/**
 * Reproducer for the DV packing failure observed when running Delta's
 * `DeletionVectorsSuite."DELETE with DVs - on a table with no prior DVs"` under the
 * Gluten classpath.
 *
 * Root cause: Gluten's [[DeltaSQLCommandTest]] mixin sets `spark.sql.shuffle.partitions=5`.
 * Delta's DV writer in [[org.apache.spark.sql.delta.commands.DMLWithDeletionVectorsHelper]]
 * does `groupBy(filePath).agg(...).mapPartitions(writeDVFile)` — one DV file per non-empty
 * post-shuffle partition. With 5 partitions all carrying DV-emitting rows, you get 5 DV
 * files; vanilla Delta assumes 1 (its tests run with shuffle.partitions=200 + AQE coalesce
 * that lands on 1).
 *
 * Two tests:
 *   1. Reproduces the failure under Gluten defaults (expects 5 DV files).
 *   2. Demonstrates that forcing the post-aggregate to a single partition (via
 *      `spark.sql.shuffle.partitions=1`) restores the vanilla-Delta-expected packing.
 *
 * NOTE: To run this end-to-end requires the Velox native library loaded; otherwise
 * Gluten falls back to vanilla Spark and the second test trivially passes while the
 * first will produce 1 file (no repro).
 */
class GlutenDeletionVectorPackingSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaExcludedTestMixin
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils {

  import testImplicits._

  override def excluded: Seq[String] = Nil

  /** Mirror of Delta `DeletionVectorsSuite."DELETE with DVs - on a table with no prior DVs"`. */
  private def runDeleteAndCountDvFiles(): (Int, Int, Long) = {
    val path = java.io.File.createTempFile("gluten-dv-pack-", "").getAbsolutePath
    new java.io.File(path).delete()
    try {
      // Enable DV-based DELETE on new tables.
      withSQLConf(
        "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true") {
        // Create a table with 500 files of 2 rows each.
        val numFiles = 500
        spark.range(0, 1000, step = 1, numPartitions = numFiles)
          .write.format("delta").save(path)
        val log = DeltaLog.forTable(spark, path)
        val tableName = s"delta.`$path`"

        // DV-targeted DELETE: 100 rows across 100 distinct files (id < 200, id even).
        spark.sql(s"DELETE FROM $tableName WHERE id % 2 = 0 AND id < 200")

        val snap = log.update()
        val all = snap.allFiles.collect()
        val withDvs = all.filter(_.deletionVector != null)
        val distinctDvFiles = withDvs
          .map(_.deletionVector.absolutePath(new Path(path)))
          .toSet
        (all.length, withDvs.length, distinctDvFiles.size.toLong)
      }
    } finally {
      // Best-effort cleanup.
      try { org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(path)) }
      catch { case _: Throwable => /* ignore */ }
    }
  }

  test("DV packing under default Gluten test config (expected to produce 5 DV files)") {
    val (totalFiles, dvFiles, distinctDvBins) = runDeleteAndCountDvFiles()
    // Sanity: 500 files in, 100 should now have DVs.
    assert(totalFiles === 500, s"unexpected total file count: $totalFiles")
    assert(dvFiles === 100, s"unexpected count of files-with-DVs: $dvFiles")

    // The reproduction of the failure: with shuffle.partitions=5 from the
    // Gluten DeltaSQLCommandTest mixin, the DV writer's post-aggregate
    // mapPartitions lands on 5 non-empty partitions, producing 5 DV files
    // rather than the 1 vanilla Delta expects.
    assert(distinctDvBins === 5L,
      s"Expected 5 DV files under shuffle.partitions=5; got $distinctDvBins. " +
      "If this is now 1, the issue has been fixed (or Velox native is not loaded " +
      "and Gluten fell back to vanilla Spark, in which case AQE coalesces to 1).")
  }

  test("DV packing with shuffle.partitions=1 restores vanilla Delta behavior (1 DV file)") {
    withSQLConf("spark.sql.shuffle.partitions" -> "1") {
      val (totalFiles, dvFiles, distinctDvBins) = runDeleteAndCountDvFiles()
      assert(totalFiles === 500)
      assert(dvFiles === 100)
      assert(distinctDvBins === 1L,
        s"Expected 1 DV file with shuffle.partitions=1; got $distinctDvBins")
    }
  }
}
