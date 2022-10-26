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

package windows

import com.spotify.scio.pubsub._
import com.spotify.scio.values.WindowOptions
import com.spotify.scio.{Args, ContextAndArgs, ScioContext, streaming}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.decode
import org.apache.beam.sdk.transforms.windowing.{AfterFirst, AfterPane, AfterProcessingTime, Repeatedly}
import org.joda.time.Duration
import org.slf4j.LoggerFactory


object GlobalWindows {

  private val log = LoggerFactory.getLogger(this.getClass)

  case class Taxi(
                   ride_status: String,
                   meter_reading: Double
                 )

  implicit val taxiRideDecoder: Decoder[Taxi] = deriveDecoder[Taxi]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val count = opts.getOrElse("count", "100")
    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    // Global window trigger
    // When using GlobalWindows be careful of not keeping the state forever and clearing state. In this example we
    // add a compound trigger so that we clean up state and don't let any window without triggering (e.g., we trigger
    // every 100 elements but we only got 99 for that key). Check Java's `testingWindows` folder  for more info.
    def customWindowOptions: WindowOptions =
      WindowOptions(
        trigger = Repeatedly.forever(AfterFirst.of(
          AfterPane.elementCountAtLeast(count.toInt),
          AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60))
        )),
        accumulationMode = streaming.DISCARDING_FIRED_PANES
      )

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .withName("Parse JSON")
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      .filter { t: Taxi => t.ride_status.equals("dropoff") }
      .withGlobalWindow(customWindowOptions)
      .map { t: Taxi => t.meter_reading }
      .sum
      .map { s => s"Price of ~$count rides was $s" }
      .withName("log")
      .map {
        log.info(_)
      }


    sc.run()
  }
}
