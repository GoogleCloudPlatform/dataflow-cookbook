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


object Join {

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
      ("Sophia", "Tennis"),
      ("Rebeca", "Ping Pong")
    )

    val createJobs: SCollection[(String, String)] = sc.withName("jobs").parallelize(jobs)
    val createHobbies: SCollection[(String, String)] = sc.withName("hobbies").parallelize(hobbies)

    // SCio has multiple ways to join

    // leftOuterJoin (and similar) use CoGroupByKey under the hood to join,
    // this means you shuffle both SCollections
    createJobs
      .withName("leftJoin")
      .leftOuterJoin(createHobbies)
      .map { kv =>
        // match handles the Some's
        val hobbieLog = kv._2._2 match {
          case None => log.info(s"${kv._1} has job ${kv._2._1}")
          case Some(i) => log.info(s"${kv._1} has job ${kv._2._1} and hobbie ${i}")
        }
      }

    // hashFullOuterJoin (and similar) use SideInputs under the hood to join
    createJobs
      .withName("hashFullJoin")
      .hashFullOuterJoin(createHobbies)
      .map { kv =>
        val key = kv._1

        kv._2 match {
          case (Some(j), Some(h)) => log.info(s"${key}, value: job $j, hobbie $h")
          case (Some(j), _) => log.info(s"${key}, value: job $j")
          case (_, Some(h)) => log.info(s"${key}, value: hobbie $h")
          case (_, _) => log.info(s"${key}")
        }
      }


    sc.run()
  }
}
