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
import org.apache.beam.sdk.state._
import org.apache.beam.sdk.transforms.DoFn.{OnTimer, ProcessElement, StateId, TimerId}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters.IterableHasAsScala

object TimerDoFn {

  private val log = LoggerFactory.getLogger(this.getClass)

  // Input type need to be KV since DoFn
  type DoFnT = DoFn[KV[String, String], String]

  class TimerFn extends DoFnT {
    // Java's GroupIntoBatches PTransform implements a similar logic
    private val BUFFER_TIME = Duration.standardSeconds(180)

    @StateId("count") private val countState = StateSpecs.value[JInt]()
    @StateId("buffer") private val bagState = StateSpecs.bag[String]()
    @TimerId("timer") private val timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

    @ProcessElement
    def processElement(c: DoFnT#ProcessContext,
                       @TimerId("timer") timer: Timer,
                       @StateId("count") countState: ValueState[JInt],
                       @StateId("buffer") bagState: BagState[String]) = {

      val key = c.element().getKey()
      val value = c.element().getValue()

      val count: Int = countState.read()

      // set timer if count is 0
      if (count == 0) {
        timer.offset(BUFFER_TIME).setRelative()
        log.info(s"Setting timer for $key")
      }

      bagState.add(value)
      countState.write(count + 1)
    }

    @OnTimer("timer")
    def onTimer(c: DoFnT#OnTimerContext,
                @StateId("count") countState: ValueState[JInt],
                @StateId("buffer") bagState: BagState[String]) = {
      log.info(s"Releasing buffer")
      bagState.read().asScala.map(id => c.output(id))

      // Clear state
      bagState.clear()
      countState.clear()
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
    // We need a coder for the StatefulDoFn
    implicit def beamKVCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())

    val pubsubTopic: String = opts.getOrElse("topic", "projects/pubsub-public-data/topics/taxirides-realtime")

    val pubsubRead = sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Topic))
    pubsubRead
      .withName("Parse JSON")
      .map { json: String =>
        val parsed: Either[Error, Taxi] = decode[Taxi](json)
        parsed
      }
      .collect { case Right(p) => p }
      // Since we are using applyTransform and SDFn, we need a KV and a coder.
      .map { t: Taxi => KV.of(t.ride_status, t.ride_id) }.setCoder(beamKVCoder)
      // If the output of `TimerFn` would've been a KV, we could've use `applyPerKeyDoFn` and not need a coder.
      .applyTransform(ParDo.of(new TimerFn()))

    sc.run()
  }
}
