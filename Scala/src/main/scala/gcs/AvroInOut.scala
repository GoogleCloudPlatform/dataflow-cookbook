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

import com.spotify.scio.avro._
import com.spotify.scio.coders._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.slf4j.LoggerFactory


object AvroInOut {

  private val log = LoggerFactory.getLogger(this.getClass)

  @AvroType.fromSchema(
    """{
      | "type":"record",
      | "name":"State",
      | "namespace":"scio.cookbook",
      | "doc":"Record for a US state",
      | "fields":[
      |   {"name":"name","type":"string"},
      |   {"name":"post_abbr","type":"string"}]}
    """.stripMargin)
  class State

  def main(cmdlineArgs: Array[String]): Unit = {
    val (scontext: ScioContext, opts: Args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scontext

    // pipeline option parameters
    val input: String = opts.getOrElse("input", "gs://cloud-samples-data/bigquery/us-states/*.avro")
    val output: String = opts("output")

    // Coder helps performance with GenericRecords
    implicit def genericCoder = Coder.avroGenericRecordCoder(State.schema)

    val read: SCollection[GenericRecord] = sc.avroFile(input, State.schema)

    read
      .map[GenericRecord] { record: GenericRecord =>
        val name = record.get("name").toString
        val postAbbr = record.get("post_abbr").toString
        log.info(s"The abbreviation of $name is $postAbbr")
        val newRecord = new GenericData.Record(State.schema)
        newRecord.put("name", name)
        newRecord.put("post_abbr", postAbbr.toLowerCase())
        newRecord
      }
      .saveAsAvroFile(output, schema = State.schema)

    sc.run()
  }
}
