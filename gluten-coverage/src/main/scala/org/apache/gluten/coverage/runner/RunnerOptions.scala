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

import java.io.File

case class RunnerOptions(
    matrixId: Option[String] = None,
    outputJson: File = new File("target/coverage.json"),
    outputMarkdown: File = new File("target/coverage.md"),
    baseline: Option[File] = None,
    mode: String = "full",
    timeoutSeconds: Long = 900L,
    verbose: Boolean = false,
    deltaVersionOverride: Option[String] = None,
    glutenVersionOverride: Option[String] = None,
    backendOverride: Option[String] = None
)

object RunnerOptions {

  def parse(args: Array[String]): RunnerOptions = {
    var opts = RunnerOptions()
    args.foreach {
      arg =>
        val (key, value) = splitOnce(arg)
        key match {
          case "--matrix" => opts = opts.copy(matrixId = Some(value))
          case "--output" => opts = opts.copy(outputJson = new File(value))
          case "--markdown" => opts = opts.copy(outputMarkdown = new File(value))
          case "--baseline" => opts = opts.copy(baseline = Some(new File(value)))
          case "--mode" => opts = opts.copy(mode = value)
          case "--timeout" => opts = opts.copy(timeoutSeconds = value.toLong)
          case "--verbose" => opts = opts.copy(verbose = true)
          case "--delta-version" => opts = opts.copy(deltaVersionOverride = Some(value))
          case "--gluten-version" => opts = opts.copy(glutenVersionOverride = Some(value))
          case "--backend" => opts = opts.copy(backendOverride = Some(value))
          case "--help" =>
            printUsage()
            sys.exit(0)
          case other =>
            throw new IllegalArgumentException(s"Unknown argument: $other")
        }
    }
    opts
  }

  private def splitOnce(arg: String): (String, String) = {
    val idx = arg.indexOf('=')
    if (idx < 0) (arg, "true") else (arg.substring(0, idx), arg.substring(idx + 1))
  }

  private def printUsage(): Unit = {
    // scalastyle:off println
    println(
      """Usage: CoverageRunner [options]
        |
        |  --matrix=ID            run only the matrix with the given id (e.g. --matrix=delta).
        |                         If omitted, runs all matrices discovered via ServiceLoader.
        |  --output=PATH          JSON output path (default: target/coverage.json)
        |  --markdown=PATH        Markdown output path (default: target/coverage.md)
        |  --baseline=PATH        Optional baseline JSON for diff/gate computation
        |  --mode=full|smoke      Run mode (default: full). Smoke runs a curated subset.
        |  --timeout=SECONDS      Wall-clock budget for the entire run (default: 900)
        |  --verbose              Include all entries in the markdown (default: only non-native)
        |  --delta-version=X      Override Delta version reported in the environment block
        |  --gluten-version=X     Override Gluten version reported in the environment block
        |  --backend=X            Override backend reported (default: velox)
        |""".stripMargin)
    // scalastyle:on println
  }
}
