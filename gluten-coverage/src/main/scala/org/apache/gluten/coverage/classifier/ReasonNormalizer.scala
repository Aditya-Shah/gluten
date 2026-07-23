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
package org.apache.gluten.coverage.classifier

import scala.util.matching.Regex

/**
 * Maps raw Gluten fallback-reason strings (validator messages, native-validation exceptions,
 * fallback-policy notes) to stable canonical codes so the report can aggregate the same root cause
 * across operators, entries, and runs. Raw reasons embed node names, thresholds, and stack
 * fragments; the codes strip that variance.
 *
 * The table is ordered: first match wins. Unmatched reasons map to `UNCLASSIFIED`; growing this
 * table as new reason shapes appear is the intended, cheap curation loop.
 */
object ReasonNormalizer {

  val NO_REASON_CODE: String = "NO_REASON_RECORDED"
  val UNCLASSIFIED: String = "UNCLASSIFIED"

  private val table: Seq[(Regex, String)] = Seq(
    ("(?i)deletion vector is not supported".r, "DELTA_DV_READ_NOT_SUPPORTED"),
    ("(?i)does not support ansi mode".r, "ANSI_MODE"),
    ("(?i)fallback policy is taking effect".r, "EXPAND_FALLBACK_POLICY"),
    ("(?i)fallback multi codegens".r, "MULTI_CODEGEN"),
    ("(?i)exceeded depth threshold".r, "COMPLEX_EXPRESSION_DEPTH"),
    ("(?i)\\[fallbackbyuseroptions\\]".r, "USER_CONFIG_DISABLED"),
    ("(?i)\\[fallbackbybackendsettings\\]".r, "BACKEND_SETTINGS"),
    ("(?i)\\[fallbackbytestinjects\\]".r, "TEST_INJECT"),
    ("(?i)validation failed with exception".r, "NATIVE_VALIDATION_EXCEPTION"),
    ("(?i)validation failed on node".r, "VALIDATOR_REJECTED"),
    ("(?i)schema.*(not supported|unsupported)|unsupported.*(type|schema)".r, "SCHEMA_VALIDATION"),
    ("(?i)not supported:".r, "NATIVE_NOT_SUPPORTED"),
    ("(?i)columnar table cache is disabled".r, "TABLE_CACHE_DISABLED"),
    ("(?i)gluten does not touch it or does not support it".r, "NOT_TOUCHED_BY_GLUTEN"),
    ("(?i)no reason recorded".r, NO_REASON_CODE)
  )

  def normalize(raw: String): String = {
    table
      .collectFirst { case (re, code) if re.findFirstIn(raw).isDefined => code }
      .getOrElse(UNCLASSIFIED)
  }
}
