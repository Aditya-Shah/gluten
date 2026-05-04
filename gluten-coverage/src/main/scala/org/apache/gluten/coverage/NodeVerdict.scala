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

sealed trait NodeVerdict {
  def opClass: String
  def kind: String
}

object NodeVerdict {
  case class Native(opClass: String) extends NodeVerdict {
    override def kind: String = "native"
  }

  case class Fallback(opClass: String, reason: String) extends NodeVerdict {
    override def kind: String = "fallback"
  }

  case class Adapter(opClass: String, direction: AdapterDirection, isTax: Boolean)
    extends NodeVerdict {
    override def kind: String = if (isTax) "tax-adapter" else "benign-adapter"
  }

  case class Vanilla(opClass: String) extends NodeVerdict {
    override def kind: String = "vanilla"
  }

  case class Unknown(opClass: String) extends NodeVerdict {
    override def kind: String = "unknown"
  }
}

sealed trait AdapterDirection
object AdapterDirection {
  case object ColumnarToRow extends AdapterDirection
  case object RowToColumnar extends AdapterDirection
}

case class ClassifiedNode(
    opClass: String,
    depth: Int,
    parentIndex: Option[Int],
    verdict: NodeVerdict)
