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

import com.spotify.scio.values.SideOutput
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object MultipleOutputs {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // define side outputs
    val multiplesThree = SideOutput[Int]()
    val multiplesFive = SideOutput[Int]()
    val multipleBoth = SideOutput[Int]()

    val (main, outputs) = sc
      .parallelize(1 to 30)
      // list outputs
      .withSideOutputs(multiplesThree, multiplesFive, multipleBoth)
      // n is the element, c the context
      .flatMap { (n, c) =>
        (n % 3, n % 5) match {
          case (0, 0) => c.output(multipleBoth, n); c.output(multiplesFive, n); c.output(multiplesThree, n)
          case (_, 0) => c.output(multiplesFive, n)
          case (0, _) => c.output(multiplesThree, n)
          case _ =>
        }
        // main output
        Some(n)
      }

    main.map { i => log.info(s"Main value $i") }

    outputs(multiplesThree).map { i => log.info(s"Multiple of three: $i") }
    outputs(multiplesFive).map { i => log.info(s"Multiple of five: $i") }
    outputs(multipleBoth).map { i => log.info(s"Multiple of both: $i") }


    sc.run()
  }
}
