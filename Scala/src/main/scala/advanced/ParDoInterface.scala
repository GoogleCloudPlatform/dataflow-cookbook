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

import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.KV
import org.joda.time.Instant
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


object ParDoInterface {

  private val log = LoggerFactory.getLogger(this.getClass)

  val zeroInstant = Instant.ofEpochSecond(0)
  val window = GlobalWindow.INSTANCE
  type DoFnType = DoFn[Int, KV[Int, String]]

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    sc
      .parallelize(1 to 100)
      // we need a Bean DoFn, since SCio doesn't cover the ParDo interface
      .applyTransform(ParDo.of(new DoFn[Int, KV[Int, String]] {
        var listElements = new ListBuffer[Int]()

        // private[<package>] is needed
        @Setup
        private[advanced] def setUp(): Unit = {
          log.info("Calling set up once per worker")
        }

        @StartBundle
        private[advanced] def startBundle(c: DoFnType#StartBundleContext): Unit = {
          log.info(s"Starting bundle")
          this.listElements = new ListBuffer[Int]()
        }

        @ProcessElement
        private[advanced] def processElement(c: DoFnType#ProcessContext): Unit = {
          this.listElements += c.element()
        }

        @FinishBundle
        private[advanced] def finishBundle(c: DoFnType#FinishBundleContext): Unit = {
          log.info(s"Finishing bundle with elements: ${this.listElements.toString}")
          // Output elements in FinishBundle need to have a timestamp and window (since a bundle may contain
          // different windows and timestamps). Instant Zero and GlobalWindow are the default in Batch
          this.listElements.map(x => c.output(KV.of(x, x.toString), zeroInstant, window))
        }

        @Teardown
        private[advanced] def tearDown(): Unit = {
          log.info("Tearing down worker")
        }
      }))
      .map(i => log.info(i.toString))

    sc.run()
  }
}
