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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.funsuite.AnyFunSuite

class MatrixLoaderSuite extends AnyFunSuite {

  private def mapper(): ObjectMapper = {
    val m = new ObjectMapper(new YAMLFactory())
    m.registerModule(DefaultScalaModule)
    m
  }

  test("parses a minimal YAML matrix into a MatrixSpec") {
    val yaml =
      """
        |schema_version: 1
        |matrix_id: test
        |entries:
        |  - id: e-1
        |    feature: cow-read
        |    operation: select
        |    dtype: int
        |    expected: native
        |    query_sql: "SELECT 1"
        |""".stripMargin

    val spec = mapper().readValue(yaml, classOf[MatrixSpec])
    assert(spec.schemaVersion == 1)
    assert(spec.matrixId == "test")
    assert(spec.entries.size == 1)
    val e = spec.entries.head
    assert(e.id == "e-1")
    assert(e.feature == "cow-read")
    assert(e.operation == "select")
    assert(e.dtype == "int")
    assert(e.expected == "native")
    assert(e.expectedVerdict == ExpectedVerdict.Native)
    assert(e.querySql.contains("SELECT 1"))
  }

  test("expected=fallback yields ExpectedVerdict.Fallback") {
    val yaml =
      """
        |schema_version: 1
        |matrix_id: test
        |entries:
        |  - id: e-1
        |    feature: cow-write
        |    operation: merge
        |    dtype: int
        |    expected: fallback
        |""".stripMargin
    val spec = mapper().readValue(yaml, classOf[MatrixSpec])
    assert(spec.entries.head.expectedVerdict == ExpectedVerdict.Fallback)
  }

  test("invalid expected throws") {
    val yaml =
      """
        |schema_version: 1
        |matrix_id: test
        |entries:
        |  - id: e-1
        |    feature: x
        |    operation: y
        |    dtype: z
        |    expected: bogus
        |""".stripMargin
    val spec = mapper().readValue(yaml, classOf[MatrixSpec])
    intercept[IllegalArgumentException] {
      spec.entries.head.expectedVerdict
    }
  }

  test("archived entries are flagged via isArchived") {
    val yaml =
      """
        |schema_version: 1
        |matrix_id: test
        |entries:
        |  - id: e-1
        |    feature: x
        |    operation: y
        |    dtype: z
        |    expected: native
        |    archived: true
        |""".stripMargin
    val spec = mapper().readValue(yaml, classOf[MatrixSpec])
    assert(spec.entries.head.isArchived)
  }

  test("BuildEnvironment.compare handles dotted versions correctly") {
    assert(BuildEnvironment.compare("3.5.5", "3.5") > 0) // 5 > 0 in third component
    assert(BuildEnvironment.compare("3.5", "3.5.0") == 0)
    assert(BuildEnvironment.compare("3.5.0", "3.5") == 0)
    assert(BuildEnvironment.compare("3.5", "3.6") < 0)
    assert(BuildEnvironment.compare("3.10", "3.5") > 0)
    assert(BuildEnvironment.compare("3.5.5-SNAPSHOT", "3.5.5") == 0)
  }

  test("BuildEnvironment skips entries below required Delta version") {
    val env = BuildEnvironment(
      sparkVersion = "3.5.5",
      deltaVersion = "3.2.1",
      glutenVersion = "1.6.0",
      scalaBinaryVersion = "2.12",
      jdkVersion = "17",
      backend = "velox")
    assert(!env.supportsDelta(Some("3.3")))
    assert(env.supportsDelta(Some("3.2")))
    assert(env.supportsDelta(None))
  }
}
