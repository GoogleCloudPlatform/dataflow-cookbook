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
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.decode
import org.joda.time.Duration
import org.slf4j.LoggerFactory


object SessionWindows {

  private val log = LoggerFactory.getLogger(this.getClass)

  case class Taxi(
                   ride_id: String,
                   ride_status: String,
                   meter_increment: Double
                 )

  implicit val taxiRideDecoder: Decoder[Taxi] = deriveDecoder[Taxi]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val gap = opts.getOrElse("duration", "10")
    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .withName("Parse JSON")
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      .filter { t: Taxi => t.ride_status.equals("enroute") }
      .map { t: Taxi => (t.ride_id, t.meter_increment) }
      .withSessionWindows(Duration.standardMinutes(gap.toInt))
      .sumByKey
      .map { s => s"Total cost of ride \'${s._1}\': ${s._2}" }
      .withName("log")
      .map {
        log.info(_)
      }


    sc.run()
  }
}
