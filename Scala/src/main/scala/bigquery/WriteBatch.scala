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

package bigquery

import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object WriteBatch {

  private val log = LoggerFactory.getLogger(this.getClass)

  // Define data class to write to BQ, schema is inferred
  @BigQueryType.toTable
  case class Person(
                     name: String,
                     age: Int,
                     country: String
                   )

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val table: String = opts("table")

    val elements = Seq(
      ("Carlos", 23, "Spain"),
      ("Maria", 27, "UK"),
      ("Ruben", 15, "Japan"),
      ("Juana", 38, "Ghana"),
      ("Kim", 77, "Brazil")
    )

    val create: SCollection[(String, Int, String)] = sc.parallelize(elements)

    create
      .map[Person] { row: (String, Int, String) => Person(row._1, row._2, row._3) }
      .saveAsBigQueryTable(Table.Spec(table), WRITE_APPEND, CREATE_IF_NEEDED)

    sc.run()
  }


}
