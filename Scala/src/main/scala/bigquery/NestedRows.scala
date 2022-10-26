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
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.slf4j.LoggerFactory


object NestedRows {

  private val log = LoggerFactory.getLogger(this.getClass)


  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "NULLABLE", "name": "url", "type": "STRING"},
      |    {"mode": "NULLABLE",
      |     "name": "repository",
      |     "type": "RECORD",
      |     "fields":[
      |        {"mode": "REQUIRED", "name": "forks", "type": "INTEGER"},
      |        {"mode": "NULLABLE", "name": "description", "type": "STRING"}]
      |     }
      |    ]
      |}
    """.stripMargin)
  class Git

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val table: String = opts("table")

    val query = "SELECT repository, url FROM `bigquery-public-data.samples.github_nested`"

    val bqRead = sc.bigQuerySelect(Query(query))

    bqRead
      .map(Git.fromTableRow)
      // filter empty rows
      .filter(r => (r.url != None & r.repository != None))
      .filter(r => r.repository.get.description != None)
      .map {
        nestedToNested
      }
      .saveAsBigQueryTable(Table.Spec(table), WRITE_APPEND, CREATE_IF_NEEDED)

    def nestedToNested(row: Git): Git = {
      // NULLABLE fields need a `get`
      // REQUIRED fields can be accessed without the `get`
      val repository = row.repository.get
      val newDescription: String = repository.description.get.toLowerCase

      // `Some` is needed for NULLABLE fields
      Git(row.url, Some(Repository$1(repository.forks, Some(newDescription))))
    }

    sc.run()
  }


}
