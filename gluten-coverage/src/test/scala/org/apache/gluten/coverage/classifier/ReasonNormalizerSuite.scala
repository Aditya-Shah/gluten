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

import org.scalatest.funsuite.AnyFunSuite

class ReasonNormalizerSuite extends AnyFunSuite {

  test("Delta DV read reason") {
    assert(
      ReasonNormalizer.normalize("Deletion vector is not supported in native.") ==
        "DELTA_DV_READ_NOT_SUPPORTED")
  }

  test("native validation exception with node detail collapses to one code") {
    val raw = "Validation failed with exception from: FileSourceScanExecTransformer, " +
      "reason: Not supported: something exotic."
    assert(ReasonNormalizer.normalize(raw) == "NATIVE_VALIDATION_EXCEPTION")
  }

  test("validator fail(p) message maps to VALIDATOR_REJECTED") {
    assert(
      ReasonNormalizer.normalize("[FallbackByNativeValidation] Validation failed on node Filter") ==
        "VALIDATOR_REJECTED")
  }

  test("user-config and backend-settings validators map to their own codes") {
    assert(
      ReasonNormalizer.normalize("[FallbackByUserOptions] Validation failed on node Sort") ==
        "USER_CONFIG_DISABLED")
    assert(
      ReasonNormalizer.normalize("[FallbackByBackendSettings] Validation failed on node Scan") ==
        "BACKEND_SETTINGS")
  }

  test("fallback-policy and ansi reasons") {
    assert(
      ReasonNormalizer.normalize(
        "Fallback policy is taking effect, net transition cost: 3") == "EXPAND_FALLBACK_POLICY")
    assert(ReasonNormalizer.normalize("does not support ansi mode") == "ANSI_MODE")
  }

  test("classifier default maps to NO_REASON_RECORDED") {
    assert(
      ReasonNormalizer.normalize(Classifier.NO_REASON_RECORDED) ==
        ReasonNormalizer.NO_REASON_CODE)
  }

  test("unmatched reasons map to UNCLASSIFIED") {
    assert(ReasonNormalizer.normalize("a reason nobody has seen before") == "UNCLASSIFIED")
  }
}
