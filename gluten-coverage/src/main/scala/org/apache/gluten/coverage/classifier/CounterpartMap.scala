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

/**
 * A static catalogue of vanilla Spark plan classes that have a known Gluten transformer
 * counterpart. Used by the classifier's defensive "untagged fallback" branch: a vanilla node whose
 * class appears here is reported as Fallback (with reason "untagged-fallback") rather than Vanilla,
 * so that plans where validator tagging skipped do not silently inflate the headline.
 *
 * Conservative by design: an entry being absent results in classification as Vanilla. A new Gluten
 * transformer is added here in the same PR that adds the transformer; missing entries understate
 * the fallback rate but never overstate it.
 *
 * Comparison is by simple class name, not Class[_], to avoid backend-specific classloading.
 */
private[coverage] object CounterpartMap {

  private val knownVanillaWithCounterpart: Set[String] = Set(
    "FilterExec",
    "ProjectExec",
    "HashAggregateExec",
    "SortAggregateExec",
    "ObjectHashAggregateExec",
    "SortExec",
    "ShuffleExchangeExec",
    "BroadcastExchangeExec",
    "BroadcastHashJoinExec",
    "ShuffledHashJoinExec",
    "SortMergeJoinExec",
    "BroadcastNestedLoopJoinExec",
    "ExpandExec",
    "GenerateExec",
    "GlobalLimitExec",
    "LocalLimitExec",
    "TakeOrderedAndProjectExec",
    "WindowExec",
    "UnionExec",
    "CoalesceExec",
    "RangeExec",
    "FileSourceScanExec",
    "BatchScanExec",
    "InMemoryTableScanExec",
    "DataWritingCommandExec",
    "WriteFilesExec"
  )

  def hasCounterpart(simpleClassName: String): Boolean =
    knownVanillaWithCounterpart.contains(simpleClassName)
}
