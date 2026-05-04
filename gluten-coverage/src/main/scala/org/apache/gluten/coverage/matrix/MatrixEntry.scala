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

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * One row of a coverage matrix. Authored as YAML; bound to this case class via Jackson YAML.
 *
 * Field naming uses snake_case in YAML (e.g. `query_sql`); Jackson maps via the `@JsonProperty`
 * annotations below.
 */
case class MatrixEntry(
    id: String,
    feature: String,
    operation: String,
    @JsonProperty("dtype") dtype: String,
    @JsonProperty("expected") expected: String,
    @JsonProperty("upstream_claim") upstreamClaim: Option[String],
    @JsonProperty("delta_min_version") deltaMinVersion: Option[String],
    @JsonProperty("spark_min_version") sparkMinVersion: Option[String],
    @JsonProperty("setup_sql") setupSql: Option[String],
    @JsonProperty("query_sql") querySql: Option[String],
    @JsonProperty("teardown_sql") teardownSql: Option[String],
    @JsonProperty("config") config: Option[Map[String, String]],
    @JsonProperty("scala_class") scalaClass: Option[String],
    @JsonProperty("notes") notes: Option[String],
    @JsonProperty("archived") archived: Option[Boolean]
) {

  def expectedVerdict: ExpectedVerdict = expected match {
    case "native" => ExpectedVerdict.Native
    case "fallback" => ExpectedVerdict.Fallback
    case "partial" => ExpectedVerdict.Partial
    case "unknown" => ExpectedVerdict.Unknown
    case other =>
      throw new IllegalArgumentException(
        s"Entry '$id' has invalid expected='$other'; must be native|fallback|partial|unknown")
  }

  def isArchived: Boolean = archived.contains(true)
}

sealed trait ExpectedVerdict
object ExpectedVerdict {
  case object Native extends ExpectedVerdict
  case object Fallback extends ExpectedVerdict
  case object Partial extends ExpectedVerdict
  case object Unknown extends ExpectedVerdict
}

/** The top-level YAML document is `{ schema_version: 1, matrix_id: "delta", entries: [...] }`. */
case class MatrixSpec(
    @JsonProperty("schema_version") schemaVersion: Int,
    @JsonProperty("matrix_id") matrixId: String,
    @JsonProperty("entries") entries: Seq[MatrixEntry]
)
