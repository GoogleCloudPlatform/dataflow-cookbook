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


object CombinePerKey {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    val create: SCollection[Int] = sc.parallelize(1 to 100)

    create
      .withName("calculateDivisors")
      .flatMap { n => (1 to n).filter { x => n % x == 0 }.map { x => (n, x) } }
      // reduceByKey applies a simple combine operation to the values of a KV
      .reduceByKey(_ + _)
      .map { kv =>
        val isPerfect = kv._2.toFloat / kv._1 match {
          case 2 => "is a perfect number "
          case _ => "is not a perfect number"
        }
        log.info(s"${kv._1} $isPerfect")
      }


    sc.run()
  }
}
