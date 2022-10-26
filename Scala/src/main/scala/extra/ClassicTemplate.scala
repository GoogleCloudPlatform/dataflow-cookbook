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

package extra

import com.spotify.scio.ScioContext
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.{Default, Description, PipelineOptionsFactory, ValueProvider}
import org.slf4j.LoggerFactory

object ClassicTemplate {
  /*
  This example shows how to use Classic Templates. Note that Flex Templates
  are the preferred method.
   */

  private val log = LoggerFactory.getLogger(this.getClass)

  trait TemplateOptions extends DataflowPipelineOptions {
    @Description("BigQuery Table")
    @Default.String("bigquery-public-data:census_bureau_usa.population_by_zip_2010")
    def getTable: ValueProvider[String]

    def setTable(value: ValueProvider[String]): Unit
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    PipelineOptionsFactory.register(classOf[TemplateOptions])
    val options = PipelineOptionsFactory.fromArgs(cmdlineArgs: _*).withValidation.as(classOf[TemplateOptions])
    options.setStreaming(false)

    val sc = ScioContext(options)

    // Since we are using ValueProviders, we need custom IO
    val customIO = BigQueryIO.readTableRows()
      // notice is options.getTable and not options.getTable() as in Java
      .from(options.getTable)
      .withoutValidation()
      .withTemplateCompatibility()

    val read = sc.customInput("BigQuery read", customIO)

    read
      .count
      .map { c: Long => log.info(s"Total count $c") }

    sc.run()
  }


}
