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
package org.apache.gluten.coverage.matrix

/**
 * Service Provider Interface for coverage matrix descriptors. Each integration that wants to be
 * measured by the coverage tool registers an implementation via
 * `META-INF/services/org.apache.gluten.coverage.matrix.MatrixDescriptor` in its jar.
 *
 * The runner discovers descriptors at startup via `java.util.ServiceLoader`.
 */
trait MatrixDescriptor {

  /** Stable identifier (e.g. "delta", "iceberg", "hudi"). Used in the report. */
  def id: String

  /** Resource path of the YAML matrix file, relative to the classpath. */
  def matrixResourcePath: String

  /**
   * Cross-walk for the discrepancy report: maps `feature` labels declared in the YAML to a row
   * identifier in the integration's upstream feature-claims doc (for Delta this is
   * `apache/gluten/docs/get-started/VeloxDelta.md`). Empty map disables the discrepancy report for
   * this descriptor.
   */
  def upstreamClaimsRows: Map[String, String] = Map.empty

  /** Human-readable name for the report header. */
  def displayName: String = id
}
