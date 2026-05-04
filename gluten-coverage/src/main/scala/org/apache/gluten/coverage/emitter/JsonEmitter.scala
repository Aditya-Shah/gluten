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
package org.apache.gluten.coverage.emitter

import org.apache.gluten.coverage.report.CoverageReport

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.{File, FileWriter}

object JsonEmitter {

  private val mapper: ObjectMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .addModule(new SimpleModule())
    .enable(SerializationFeature.INDENT_OUTPUT)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .build()

  def emit(report: CoverageReport): String = mapper.writeValueAsString(report)

  def emitTo(report: CoverageReport, path: File): Unit = {
    val parent = path.getParentFile
    if (parent != null && !parent.exists()) {
      parent.mkdirs()
    }
    val w = new FileWriter(path)
    try w.write(emit(report))
    finally w.close()
  }

  def parse(json: String): CoverageReport = mapper.readValue(json, classOf[CoverageReport])

  def parseFile(path: File): CoverageReport = mapper.readValue(path, classOf[CoverageReport])
}
