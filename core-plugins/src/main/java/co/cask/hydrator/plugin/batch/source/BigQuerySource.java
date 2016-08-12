/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.json.JSONFormat;

import java.io.IOException;

/**
 * Batch source to read from a BigQuery table
 */
@co.cask.cdap.api.annotation.Plugin(type = "batchsource")
@Name("BigQuerySource")
@Description("Reads from a database table(s) using a configurable BigQuery." +
  " Outputs one record for each row returned by the query.")
public class BigQuerySource extends ReferenceBatchSource<LongWritable, JsonObject, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  private final BQSourceConfig sourceConfig;

  public BigQuerySource(BQSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.sourceConfig = config;
  }
  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    String jobname = "BigQueryJob";
    JobConf conf = new JobConf();
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, sourceConfig.projectId);
    conf.set(BigQueryConfiguration.INPUT_QUERY_KEY, sourceConfig.importQuery);
    // Make sure the required export-bucket setting is present.
    if (Strings.isNullOrEmpty(conf.get(BigQueryConfiguration.GCS_BUCKET_KEY))) {
      LOG.warn("Missing config for '{}'; trying to default to fs.gs.system.bucket.",
               BigQueryConfiguration.GCS_BUCKET_KEY);
      String systemBucket = conf.get("fs.gs.system.bucket");
      if (Strings.isNullOrEmpty(systemBucket)) {
        LOG.error("Also missing fs.gs.system.bucket; value must be specified.");
        System.exit(1);
      } else {
        LOG.info("Setting '{}' to '{}'", BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
        conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
      }
    } else {
      LOG.info("Using export bucket '{}' as specified in '{}'",
               conf.get(BigQueryConfiguration.GCS_BUCKET_KEY), BigQueryConfiguration.GCS_BUCKET_KEY);
    }
    BigQueryConfiguration.configureBigQueryInput(conf, sourceConfig.fullyQualifiedInputTableId);
    Job job = new Job(conf, jobname);
    job.setJarByClass(BigQuerySource.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(GsonBigQueryInputFormat.class, conf)));
//    GsonBigQueryInputFormat;
  }

  @Override
  public void transform(KeyValue<LongWritable, JsonObject> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(jsonTransform(input.getValue()));
  }

  private StructuredRecord jsonTransform(JsonObject json) {
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("innerInt", Schema.of(Schema.Type.INT)),
      Schema.Field.of("innerString", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("recordField", innerSchema));

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("intField", 5)
      .set("recordField",
           StructuredRecord.builder(innerSchema)
             .set("innerInt", 7)
             .set("innerString", "hello world")
             .build()
      )
      .build();

    return record;
  }
  public static class BQSourceConfig extends PluginConfig {
    public static final String IMPORT_QUERY = "importQuery";
    public static final String PROJECT_ID   = "projectId";
    public static final String INPUT_TABLE_ID = "input_tableId";
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Name(IMPORT_QUERY)
    @Description("The SELECT query to use to import data from the specified table. ")
    @Macro
    String importQuery;

    @Name(PROJECT_ID)
    @Description("The ID of the project in Google Cloud")
    @Macro
    String projectId;

    @Name(INPUT_TABLE_ID)
    @Description("The BigQuery table to read from, in the form [optional projectId]:[datasetId].[tableId]." +
                 " Example: publicdata:samples.shakespeare")
    @Macro
    String fullyQualifiedInputTableId;
  }
}
