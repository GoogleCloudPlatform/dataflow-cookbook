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

package gcs

import com.spotify.scio.pubsub._
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.decode
import org.apache.beam.sdk.io.TextIO
import org.joda.time.Duration
import org.slf4j.LoggerFactory


object WriteTextStreaming {

  private val log = LoggerFactory.getLogger(this.getClass)

  case class Taxi(
                   ride_id: String,
                   meter_reading: Double,
                   ride_status: String
                 )

  implicit val taxiRideDecoder: Decoder[Taxi] = deriveDecoder[Taxi]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val output: String = opts("output")
    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .withName("Parse JSON")
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      .filter { t: Taxi => t.ride_status.equals("dropoff") }
      .map { t: Taxi => s"Taxi id ${t.ride_id} costed ${t.meter_reading}" }
      .withFixedWindows(Duration.standardMinutes(10))
      // We need a custom IO output
      .saveAsCustomOutput(
        "Write Streaming File(s)",
        TextIO.write()
          .withWindowedWrites()
          .withNumShards(2)
          .to(output)
      )


    sc.run()
  }
}
