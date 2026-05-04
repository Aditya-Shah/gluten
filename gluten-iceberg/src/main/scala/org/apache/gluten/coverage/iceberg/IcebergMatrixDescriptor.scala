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
package org.apache.gluten.coverage.iceberg

import org.apache.gluten.coverage.matrix.MatrixDescriptor

/**
 * Coverage matrix descriptor for Apache Iceberg on Gluten + Velox. Registered via
 * `META-INF/services/org.apache.gluten.coverage.matrix.MatrixDescriptor`.
 *
 * The cross-walk in `upstreamClaimsRows` maps the matrix's `feature` labels to row labels in
 * `apache/gluten/docs/get-started/VeloxIceberg.md`. Discrepancies between the upstream claim and
 * the measured verdict are surfaced separately in the report so documentation drift is visible.
 */
class IcebergMatrixDescriptor extends MatrixDescriptor {

  override def id: String = "iceberg"

  override def displayName: String = "Apache Iceberg"

  override def matrixResourcePath: String = "coverage/iceberg-matrix.yaml"

  override def upstreamClaimsRows: Map[String, String] = Map(
    "iceberg-read" -> "Read data",
    "iceberg-read-partitioned" -> "Read data (partitioned)",
    "iceberg-read-metadata" -> "Read metadata",
    "iceberg-write" -> "Writing",
    "iceberg-write-mor" -> "Writing (merge-on-read)",
    "iceberg-merge" -> "SQL Extensions",
    "iceberg-maintenance" -> "SQL Extensions",
    "iceberg-time-travel" -> "Read data",
    "iceberg-types" -> "DataType",
    "iceberg-transforms" -> "Read data (partitioned)",
    "iceberg-schema-evolution" -> "Schema evolution",
    "iceberg-catalog" -> "Adding catalogs",
    "iceberg-format" -> "Format",
    "iceberg-branch-tag" -> "SQL Extensions"
  )
}
