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
package org.apache.gluten.coverage

case class PlanReport(nodes: Seq[ClassifiedNode]) {

  def nodeCount: Int = nodes.size

  def nativeCount: Int = nodes.count(_.verdict.isInstanceOf[NodeVerdict.Native])

  def fallbackCount: Int = nodes.count(_.verdict.isInstanceOf[NodeVerdict.Fallback])

  def taxAdapterCount: Int = nodes.count {
    case ClassifiedNode(_, _, _, _, a: NodeVerdict.Adapter) => a.isTax
    case _ => false
  }

  def benignAdapterCount: Int = nodes.count {
    case ClassifiedNode(_, _, _, _, a: NodeVerdict.Adapter) => !a.isTax
    case _ => false
  }

  def vanillaCount: Int = nodes.count(_.verdict.isInstanceOf[NodeVerdict.Vanilla])

  def neutralCount: Int = nodes.count(_.verdict.isInstanceOf[NodeVerdict.Neutral])

  def unknownCount: Int = nodes.count(_.verdict.isInstanceOf[NodeVerdict.Unknown])

  def fallbackReasons: Seq[String] = nodes.collect {
    case ClassifiedNode(_, _, _, _, f: NodeVerdict.Fallback) => f.reason
  }.distinct

  def isPureNative: Boolean = fallbackCount == 0 && unknownCount == 0
}

object PlanReport {
  val empty: PlanReport = PlanReport(Seq.empty)

  def merge(a: PlanReport, b: PlanReport): PlanReport = PlanReport(a.nodes ++ b.nodes)
}
