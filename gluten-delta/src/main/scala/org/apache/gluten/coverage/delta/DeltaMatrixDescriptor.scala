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
package org.apache.gluten.coverage.delta

import org.apache.gluten.coverage.matrix.MatrixDescriptor

/**
 * Coverage matrix descriptor for Delta Lake on Gluten + Velox. Registered via
 * `META-INF/services/org.apache.gluten.coverage.matrix.MatrixDescriptor`.
 *
 * The cross-walk in `upstreamClaimsRows` maps the matrix's `feature` labels to row labels in
 * `apache/gluten/docs/get-started/VeloxDelta.md`. Discrepancies between the upstream claim and the
 * measured verdict are surfaced separately in the report so documentation drift is visible.
 */
class DeltaMatrixDescriptor extends MatrixDescriptor {

  override def id: String = "delta"

  override def displayName: String = "Delta Lake"

  override def matrixResourcePath: String = "coverage/delta-matrix.yaml"

  override def upstreamClaimsRows: Map[String, String] = Map(
    "cow-read" -> "Basic functionality",
    "cow-write" -> "Basic functionality",
    "cow-maintenance" -> "Basic functionality",
    "table-feature-cdc" -> "Change data feed",
    "table-feature-check" -> "CHECK constraints",
    "table-feature-generated-cols" -> "Generated columns",
    "table-feature-column-mapping" -> "Column mapping",
    "table-feature-identity" -> "Identity columns",
    "table-feature-row-tracking" -> "Row tracking",
    "table-feature-dv" -> "Deletion vectors",
    "table-feature-tsntz" -> "TimestampNTZ",
    "table-feature-liquid" -> "Liquid clustering",
    "table-feature-type-widening" -> "Type widening",
    "table-feature-variant" -> "Variant",
    "table-feature-variant-shredding" -> "Variant shredding",
    "table-feature-collations" -> "Collations",
    "table-feature-protected-checkpoints" -> "Protected checkpoints"
  )
}
