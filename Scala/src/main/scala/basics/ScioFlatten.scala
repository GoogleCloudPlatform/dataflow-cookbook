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

// NOTE: THIS IS NOT THE TYPICAL BEAM FLATTEN, this is the equivalen of Flatten.iterables
// check file Flatten for that
object ScioFlatten {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // generate 4 elements that are sequences
    val elements = Seq(
      Seq("Carlos", "Maria", "Ruben"),
      Seq("Car", "Bike", "Plane", "Boat", "Hovercraft"),
      Seq("Dog", "Cat"),
      Seq("Dataflow")
    )

    val create: SCollection[Seq[String]] = sc.parallelize(elements)

    // flatten takes Seq of objects and send its values once by one as single elements
    // same as FlattenIterables
    create
      .flatten
      .withName("log")
      .map {
        log.info(_)
      }


    sc.run()
  }
}
