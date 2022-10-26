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

package advanced

import com.spotify.scio.pubsub._
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}

import java.lang.{Integer => JInt}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser.decode
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.state.{BagState, StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.IterableHasAsScala


object StatefulDoFn {

  private val log = LoggerFactory.getLogger(this.getClass)

  // Input and output types need to be KVs since applyPerKeyDoFn
  type DoFnT = DoFn[KV[String, String], KV[String, String]]

  class StatefulFn extends DoFnT {
    // Java's GroupIntoBatches PTransfrom implements a similar logic
    // When using StatefulDoFns be careful of not keeping the state forever and clearing state. This example is OK
    // because we know the keys are always incoming, but if we have  sparse keys, we may keep the buffer up forever
    // (e.g., we trigger every 100 elements but we only got 99 for that key. See TimerDoFn for an example that
    // would fix that)
    private val BUFFER_SIZE: Map[String, Int] = Map("enroute" -> 100000, "dropoff" -> 100, "pickup" -> 100)

    @StateId("count") private val countState = StateSpecs.value[JInt]()
    @StateId("buffer") private val bagState = StateSpecs.bag[String]()

    @ProcessElement
    def processElement(c: DoFnT#ProcessContext,
                       @StateId("count") countState: ValueState[JInt],
                       @StateId("buffer") bagState: BagState[String]) = {

      val key = c.element().getKey()
      val value = c.element().getValue()

      val count: Int = countState.read()
      countState.write(count + 1)
      bagState.add(value)


      if ((count + 1) >= BUFFER_SIZE(key)) {
        log.info(s"Releasing buffer for key $key with ${count + 1} elements")
        bagState.read().asScala.map(id => c.output(KV.of(key, id)))

        // Clear state
        bagState.clear()
        countState.clear()
      }
    }
  }

  case class Taxi(
                   ride_id: String,
                   ride_status: String
                 )

  implicit val taxiRideDecoder: Decoder[Taxi] = deriveDecoder[Taxi]


  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))

    pubsubRead
      .withName("Parse JSON")
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      // Since we are using applyPerKeyDoFn and SDFn, we need a KV
      .map { t: Taxi => (t.ride_status, t.ride_id) }
      // If the output of `StatefulFn` would've not been a KV, we should've use `applyTransform` and need a coder.
      .applyPerKeyDoFn(new StatefulFn)


    sc.run()
  }
}
