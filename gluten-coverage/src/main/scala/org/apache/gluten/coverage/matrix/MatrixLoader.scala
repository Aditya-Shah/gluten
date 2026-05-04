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

import java.io.InputStream
import java.util.ServiceLoader

import scala.collection.JavaConverters._

/**
 * Loads coverage matrices from descriptors registered via java.util.ServiceLoader.
 *
 * Validation:
 *   - schema_version must equal MatrixLoader.SUPPORTED_SCHEMA_VERSION;
 *   - entry `id`s must be unique within a matrix;
 *   - archived entries are dropped silently (their id is reserved but they do not run);
 *   - entries whose `delta_min_version` or `spark_min_version` are above the running build are kept
 *     but flagged as version-skipped (loader does not run them; runner skips with `verdict =
 *     skipped(version)`).
 */
class MatrixLoader(env: BuildEnvironment) {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper(new YAMLFactory())
    m.registerModule(DefaultScalaModule)
    m
  }

  def loadAll(): Seq[LoadedMatrix] = {
    val loader = ServiceLoader.load(classOf[MatrixDescriptor])
    loader.asScala.toSeq.map(load)
  }

  def load(descriptor: MatrixDescriptor): LoadedMatrix = {
    val resource = descriptor.matrixResourcePath
    val stream: InputStream = Option(getClass.getClassLoader.getResourceAsStream(resource))
      .getOrElse(
        throw new MatrixLoadException(
          s"Matrix resource '$resource' not found on classpath for descriptor '${descriptor.id}'"))

    val spec: MatrixSpec =
      try mapper.readValue(stream, classOf[MatrixSpec])
      finally stream.close()

    if (spec.schemaVersion != MatrixLoader.SUPPORTED_SCHEMA_VERSION) {
      throw new MatrixLoadException(
        s"Matrix '${descriptor.id}' has schema_version=${spec.schemaVersion}; " +
          s"supported version is ${MatrixLoader.SUPPORTED_SCHEMA_VERSION}")
    }

    val active = spec.entries.filterNot(_.isArchived)
    val ids = active.map(_.id)
    if (ids.distinct.size != ids.size) {
      val duplicates = ids.diff(ids.distinct).distinct.sorted.mkString(", ")
      throw new MatrixLoadException(
        s"Matrix '${descriptor.id}' has duplicate entry ids: $duplicates")
    }

    LoadedMatrix(descriptor, active)
  }
}

object MatrixLoader {
  val SUPPORTED_SCHEMA_VERSION: Int = 1
}

case class LoadedMatrix(descriptor: MatrixDescriptor, entries: Seq[MatrixEntry])

class MatrixLoadException(msg: String) extends RuntimeException(msg)

/**
 * Snapshot of versions that the matrix is being run against. Used to filter version-gated entries
 * (e.g. an entry that requires Delta 3.3+ is skipped on Delta 3.2).
 */
case class BuildEnvironment(
    sparkVersion: String,
    deltaVersion: String,
    glutenVersion: String,
    scalaBinaryVersion: String,
    jdkVersion: String,
    backend: String) {

  def supportsDelta(minVersion: Option[String]): Boolean = minVersion match {
    case None => true
    case Some(min) => BuildEnvironment.compare(deltaVersion, min) >= 0
  }

  def supportsSpark(minVersion: Option[String]): Boolean = minVersion match {
    case None => true
    case Some(min) => BuildEnvironment.compare(sparkVersion, min) >= 0
  }
}

object BuildEnvironment {

  /**
   * Compare two dotted-numeric versions ("3.5.5" vs "3.5"). Each component is parsed as Int;
   * non-numeric suffixes are dropped after the first non-digit. A shorter version compares equal to
   * longer where the shared prefix matches and the longer has only zeros after.
   */
  def compare(a: String, b: String): Int = {
    val pa = parts(a)
    val pb = parts(b)
    val len = math.max(pa.length, pb.length)
    def at(seq: IndexedSeq[Int], i: Int): Int = if (i < seq.length) seq(i) else 0
    (0 until len).iterator
      .map(i => at(pa, i).compareTo(at(pb, i)))
      .find(_ != 0)
      .getOrElse(0)
  }

  private def parts(v: String): IndexedSeq[Int] =
    v.split('.').toIndexedSeq.map {
      p =>
        val digits = p.takeWhile(_.isDigit)
        if (digits.isEmpty) 0 else digits.toInt
    }
}
