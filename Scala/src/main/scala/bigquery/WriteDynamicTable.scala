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
import com.spotify.scio.bigquery.dynamic._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.slf4j.LoggerFactory

object WriteDynamicTable {

  private val log = LoggerFactory.getLogger(this.getClass)

  // Define data class to write to BQ, schema is inferred
  @BigQueryType.toTable
  case class Person(
                     name: String,
                     age: Int,
                     country: String,
                     group: Int
                   )

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val tablePrefix: String = opts("tablePrefix")

    val elements = Seq(
      ("Carlos", 23, "Spain", 1),
      ("Maria", 27, "UK", 2),
      ("Ruben", 15, "Japan", 1),
      ("Juana", 38, "Ghana", 1),
      ("Kim", 77, "Brazil", 2)
    )


    val create: SCollection[(String, Int, String, Int)] = sc.parallelize(elements)

    create
      .map { row: (String, Int, String, Int) => Person(row._1, row._2, row._3, row._4) }
      .map[TableRow](Person.toTableRow)
      .saveAsBigQuery(Person.schema, WRITE_EMPTY, CREATE_IF_NEEDED) { r =>
        val group = r.getValue.get("group").toString
        new TableDestination(s"${tablePrefix}_$group", s"table for group $group")
      }

    sc.run()
  }


}
