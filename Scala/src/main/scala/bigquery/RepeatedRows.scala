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


object RepeatedRows {

  private val log = LoggerFactory.getLogger(this.getClass)

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REQUIRED", "name": "ngram", "type": "STRING"},
      |    {"mode": "REPEATED",
      |     "name": "cell",
      |     "type": "RECORD",
      |     "fields":[
      |        {"mode": "NULLABLE", "name": "value", "type": "STRING"},
      |        {"mode": "REQUIRED", "name": "volume_count", "type": "INTEGER"}]
      |     }
      |    ]
      |}
  """.stripMargin)
  class NGram

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val table: String = opts("table")

    val query = "SELECT ngram, cell FROM `bigquery-public-data.samples.trigrams` LIMIT 100000"

    val bqRead = sc.bigQuerySelect(Query(query))

    bqRead
      .map(NGram.fromTableRow)
      .map {
        repeatedToNewRepeated
      }
      .filter { r: NGram => !r.cell.isEmpty }
      .saveAsBigQueryTable(Table.Spec(table), WRITE_APPEND, CREATE_IF_NEEDED)

    def repeatedToNewRepeated(row: NGram) = {
      val ngram = row.ngram
      val cellRepeated = row.cell

      // Cell$1 is created from the schema
      val filterCellRepeated: List[Cell$1] = cellRepeated
        .filter(cell => cell.volume_count > 3)
        .map(cell => Cell$1(Some(cell.value.get.replaceAll(raw"[\[\]]", "")), cell.volume_count))

      NGram(ngram, filterCellRepeated)
    }

    sc.run()
  }


}
