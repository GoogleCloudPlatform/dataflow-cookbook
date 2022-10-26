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
import io.circe._
import io.circe.generic.semiauto._
import org.slf4j.LoggerFactory


object WriteTopic {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameter
    val pubsubOutputTopic: String = opts("outputTopic")

    val topic = "projects/pubsub-public-data/topics/taxirides-realtime"

    // Reading from a topic creates a subscription to it automatically.
    val pubsubRead = sc.read(PubsubIO.string(topic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      // Since we read strings, we can publish directly as strings
      .write(PubsubIO.string(pubsubOutputTopic))(PubsubIO.WriteParam())


    sc.run()
  }


}
