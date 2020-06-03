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

import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.data.HCatRecord;
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
 * The {@link HiveToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Kafka, executes a UDF, and outputs the resulting records to BigQuery. Any errors which occur
 * in the transformation of the data or execution of the UDF will be output to a separate errors
 * table in BigQuery. The errors table will be created if it does not exist prior to execution. Both
 * output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Kafka topic exists and the message is encoded in a valid JSON format.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-bigquery
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.KafkaToBigQuery \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=kafka-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "bootstrapServers=my_host:9092,inputTopic=kafka-test,\
 * outputTableSpec=kafka-test:kafka.kafka_to_bigquery,\
 * outputDeadletterTable=kafka-test:kafka.kafka_to_bigquery_deadletter"
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

    @Description("Output topic to write to")
    String getOutputTable();

    void setOutputTable(String value);

    @Description("Temporary directory for BigQuery loading process")
    String getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(String value);
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

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "hdfs:///");
    hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hadoopConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs" +
            ".GoogleHadoopFileSystem");
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop" +
            ".fs.gcs.GoogleHadoopFS");
    options.setHdfsConfiguration(Collections.singletonList(hadoopConf));
    Pipeline pipeline = Pipeline.create(options);

    Map<String, String> configProperties = new HashMap<String, String>();
    configProperties.put("hive.metastore.uris", options.getMetastoreUri());
    PCollection<Row> p =
        pipeline
            .apply(
                "Read from Hive source",
                    HCatToRow.fromSpec(
                            HCatalogIO.read()
                                    .withConfigProperties(configProperties)
                                    .withDatabase(options.getHiveDatabaseName())
                                    .withTable(options.getHiveTableName())));
                            //.withPartition(partitionValues)
                            //.withBatchSize(1024L)
                            //.withFilter(filterString)
            //.apply(new HCatToRow());
    p.apply(BigQueryIO.<Row>write()
            .withSchema(BigQueryUtils.toTableSchema(p.getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .to(options.getOutputTable())
    );
    //  p = p.apply("print", MapElements.<Row, Row>via(
    //          new SimpleFunction<Row, Row>(){
    //            @Override
    //            public Row apply(Row r) {
    //              //try {
    //              System.out.println(r);
    //              System.out.println(r.getClass().getName());
    //              return r;
    //              //}
    //              /*catch (IOException e) {
    //                throw new RuntimeException("aaa", e);
    //              }*/
    //            }
    //          }
    //  ));

            /*
             * Step #2: Transform the Kafka Messages into TableRows
             */
            //.apply("ConvertMessageToTableRow", new MessageToTableRow
    // (options));

    /*
     * Step #3: Write the successful records out to BigQuery
     */
    //transformOut
    //    .get(TRANSFORM_OUT)
    //    .apply(
    //        "WriteSuccessfulRecords",
    //        BigQueryIO.writeTableRows()
    //            .withoutValidation()
    //            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
    //            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
    //            .to(options.getOutputTableSpec()));

    ///*
    // * Step #4: Write failed records out to BigQuery
    // */
    //PCollectionList.of(transformOut.get(UDF_DEADLETTER_OUT))
    //    .and(transformOut.get(TRANSFORM_DEADLETTER_OUT))
    //    .apply("Flatten", Flatten.pCollections())
    //    .apply(
    //        "WriteFailedRecords",
    //        WriteKafkaMessageErrors.newBuilder()
    //            .setErrorRecordsTable(
    //                ValueProviderUtils.maybeUseDefaultDeadletterTable(
    //                    options.getOutputDeadletterTable(),
    //                    options.getOutputTableSpec(),
    //                    DEFAULT_DEADLETTER_TABLE_SUFFIX))
    //            .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
    //            .build());
    return pipeline.run();
  }
}
