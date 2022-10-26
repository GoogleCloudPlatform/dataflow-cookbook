/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package basics

import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object MapSideInput {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    val elements = Seq(
      ("USD", 3.1415),
      ("USD", 1729.0),
      ("CHF", 2.7182),
      ("EUR", 1.618),
      ("CHF", 1.1),
      ("CHF", 342.45),
      ("EUR", 890.01)
    )

    val rates = Seq(
      ("USD", 1.0),
      ("EUR", 0.8),
      ("CHF", 0.9)
    )

    val values: SCollection[(String, Double)] = sc.parallelize(elements)
    val ratesSCol: SCollection[(String, Double)] = sc.parallelize(rates)

    val rateSide = ratesSCol.asMapSideInput // Creates a SideInput as Map

    values
      .withSideInputs(rateSide) // point to side input (Map)
      .map { (kv, s) =>
        // kv is the main value, s the side input
        // to retrieve the side input we need, we do s(SideInput Variable)
        val rate: Double = s(rateSide).getOrElse(kv._1, 1.0)
        val changed = s"${kv._2} ${kv._1} are ${kv._2 * rate} USD"
        changed
      }
      .toSCollection // This is needed so that the Side Input is not kept in the next operation
      .withName("log")
      .map { s: String => log.info(s) }.withName("log")


    sc.run()
  }
}
