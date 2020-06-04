/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.common.JavascriptTextTransformer;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.IOException;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.auto.value.AutoValue;
//import com.google.cloud.teleport.coders.FailsafeElementCoder;
//import com.google.cloud.teleport.kafka.connector.KafkaIO;
//import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
//import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
//import com.google.cloud.teleport.util.DualInputNestedValueProvider;
//import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
//import com.google.cloud.teleport.util.ResourceUtils;
//import com.google.cloud.teleport.util.ValueProviderUtils;
//import com.google.cloud.teleport.values.FailsafeElement;
//import org.apache.beam.sdk.coders.CoderRegistry;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.io.hcatalog.HCatToRow;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
//import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
//import org.apache.beam.sdk.options.ValueProvider;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.joda.time.DateTimeZone;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;

/**
 * The {@link HiveToBigQuery} pipeline is a batch pipeline which ingests data
 * from Hive and outputs the resulting records to BigQuery.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Hive input table exists.
 *   <li>The BigQuery output table exists.
 *   <li>The Hive metastore and HDFS URI are reachable from the Dataflow owrker
 *   machines.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 *
 * # Set some environment variables
 * PROJECT=my-project
 * TEMP_BUCKET=my-temp-bucket
 * INPUT_HIVE_DATABASE=mydatabase
 * INPUT_HIVE_TABLE=mytable
 * OUTPUT_TABLE=${PROJECT}:my_dataset.my_table
 * PARTITION_COLS=["col1","col2"]
 * FILTER_STRING=my-string
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create an image spec in GCS that contains the path to the image
 * {
 *    "docker_template_spec": {
 *       "docker_image": $TARGET_GCR_IMAGE
 *     }
 *  }
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
 * JOB_NAME="kafka-to-bigquery`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST -H "Content-Type: application/json"     \
 *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *     "${TEMPLATES_LAUNCH_API}"`
 *     `"?validateOnly=false"`
 *     `"&dynamicTemplate.gcsPath=${TEMP_BUCKET}/path/to/image-spec"`
 *     `"&dynamicTemplate.stagingLocation=${TEMP_BUCKET}/staging" \
 *     -d '
 *      {
 *       "jobName":"'$JOB_NAME'",
 *       "parameters": {
 *           "metastoreUri":"thrift://my_host:9083",
 *           "hiveDatabaseName":"'$INPUT_HIVE_DATABASE'",
 *           "hiveTableName":"'$INPUT_HIVE_TABLE'",
 *           "partitionCols":"'$PARTITION_COLS'",
 *           "filterString":"'$FILTER_STRING'",
 *           "outputTable":"'$OUTPUT_TABLE'"
 *        }
 *       }
 *      '
 * </pre>
 */
public class HiveToBigQuery {

  /** The log to output status messages. */
  private static final Logger LOG = LoggerFactory.getLogger(HiveToBigQuery.class);

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends HadoopFileSystemOptions,
          JavascriptTextTransformerOptions {
    @Description("hive metastore uri")
    String getMetastoreUri();

    void setMetastoreUri(String value);

    @Description("hive database name")
    String getHiveDatabaseName();

    void setHiveDatabaseName(String value);

    @Description("hive table name")
    String getHiveTableName();

    void setHiveTableName(String value);

    @Description("the names of the columns that are partitions")
    List<java.lang.String> getPartitionCols();

    void setPartitionCols(String value);

    @Description("the filter details")
    String getFilterString();

    void setFilterString(String value);

    @Description("Output topic to write to")
    String getOutputTable();

    void setOutputTable(String value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * HiveToBigQuery#run(Options)} method to start the pipeline and invoke
   * {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */

  public static PipelineResult run(Options options) {

    // Set Hadoop configurations
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "hdfs:///");
    hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hadoopConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs" +
            ".GoogleHadoopFileSystem");
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop" +
            ".fs.gcs.GoogleHadoopFS");
    options.setHdfsConfiguration(Collections.singletonList(hadoopConf));

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    Map<String, String> configProperties = new HashMap<String, String>();
    configProperties.put("hive.metastore.uris", options.getMetastoreUri());
    PCollection<Row> p =
        pipeline
            /*
             * Step #1: Read hive table rows from Hive.
             */
            .apply(
                "Read from Hive source",
                    HCatToRow.fromSpec(
                            HCatalogIO.read()
                                    .withConfigProperties(configProperties)
                                    .withDatabase(options.getHiveDatabaseName())
                                    .withTable(options.getHiveTableName())
                                    .withPartitionCols(options.getPartitionCols())
                                    .withFilter(options.getFilterString())));
    /*
     * Step #2: Write table rows out to BigQuery
     */
    p.apply(
            "Write records to BigQuery",
            BigQueryIO.<Row>write()
            .withSchema(BigQueryUtils.toTableSchema(p.getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withExtendedErrorInfo()
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .to(options.getOutputTable())
    );
    return pipeline.run();
  }
}
