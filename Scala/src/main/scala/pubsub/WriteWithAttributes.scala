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

package pubsub

import com.spotify.scio.pubsub._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory

object WriteWithAttributes {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameter
    val pubsubOutputTopic: String = opts("outputTopic")

    val elements = Seq(
      ("Carlos", 27, "Spain"),
      ("Maria", 27, "UK"),
      ("Ruben", 15, "Japan"),
      ("Juana", 38, "Ghana"),
      ("Kim", 77, "Brazil")
    )

    val create: SCollection[(String, Int, String)] = sc.parallelize(elements)

    val pubsubMessages = create
      .map { element: (String, Int, String) =>
        // Map has to be [String, String]
        val attributesMap = Map(
          "age" -> element._2.toString,
          "country" -> element._3
        )
        (element._1, attributesMap)
      }
      .write(PubsubIO.withAttributes[String](pubsubOutputTopic))(PubsubIO.WriteParam())

    sc.run()
  }


}
