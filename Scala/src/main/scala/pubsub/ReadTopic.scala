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
import io.circe.parser.decode
import org.slf4j.LoggerFactory


object ReadTopic {

  private val log = LoggerFactory.getLogger(this.getClass)

  // Schema for parsing
  case class Taxi(
                   ride_id: String,
                   timestamp: String,
                   meter_reading: Double,
                   ride_status: String,
                   passenger_count: Int
                 )

  implicit val taxiRideDecoder: Decoder[Taxi] = deriveDecoder[Taxi]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      .filter { t => t.ride_status == "dropoff" }
      .map { ride =>
        log.info(s"${ride.ride_id} costed ${ride.meter_reading} for ${ride.passenger_count} passengers")
      }


    sc.run()
  }


}
