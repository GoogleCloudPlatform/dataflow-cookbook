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

package basics

import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object CoGroupByKey {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    val jobs = Seq(
      ("Anna", "SWE"),
      ("Kim", "Data Engineer"),
      ("Kim", "Data Scientist"),
      ("Robert", "Artist"),
      ("Sophia", "CEO"),
      ("Ruben", "Writer")
    )

    val hobbies = Seq(
      ("Anna", "Painting"),
      ("Kim", "Football"),
      ("Kim", "Gardening"),
      ("Robert", "Swimming"),
      ("Sophia", "Mathematics"),
      ("Sophia", "Tennis")
    )

    val createJobs: SCollection[(String, String)] = sc.parallelize(jobs)
    val createHobbies: SCollection[(String, String)] = sc.parallelize(hobbies)

    createJobs
      .cogroup(createHobbies)
      .map { kv => (kv._1, s"Jobs: ${kv._2._1.mkString(", ")} and Hobbies: ${kv._2._2.mkString(", ")}") }
      .withName("log")
      .map { kv => log.info(s"Key: ${kv._1}; Cogrouped-Value: ${kv._2}") }

    sc.run()
  }
}
