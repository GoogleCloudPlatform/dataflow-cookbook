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
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object ReadWithAttributes {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    // messages optionally have attributes "key" and "country" (Strings)
    val pubsubTopic: String = opts("topic")

    val pubsubRead = sc.read(PubsubIO.withAttributes[String](pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .map { message =>
        val payload = new String(message._1)
        val attributes = message._2
        val age = attributes.get("age")
        val country = attributes.get("country")

        val string = (age, country) match {
          case (Some(a), Some(c)) => s" has age $a and is from $c."
          case (Some(a), _) => s" has age $a."
          case (_, Some(c)) => s" is from $c."
          case (_, _) => "."
        }

        s"$payload$string"
      }
      .map { p: String => log.info(p) }


    sc.run()
  }


}
