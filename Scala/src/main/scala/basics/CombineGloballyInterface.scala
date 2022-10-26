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


object CombineGloballyInterface {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    val create: SCollection[Int] = sc.parallelize(1 to 10)

    create
      // Calculate avg
      // 0.0, 0 is the first accumulator
      .aggregate(0.0, 0)(
        (acc, x) => (acc._1 + x, acc._2 + 1), // add input
        (l, r) => (l._1 + r._1, l._2 + r._2) // merge accumulators, type kv
      )
      .map { kv => kv._1 / kv._2 }
      .withName("log")
      .map { x => log.info("Avg is " + x) }


    sc.run()
  }
}
