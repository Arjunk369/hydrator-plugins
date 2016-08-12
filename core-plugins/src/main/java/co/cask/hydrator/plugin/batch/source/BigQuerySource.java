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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.gson.JsonObject;
import org.apache.hadoop.io.LongWritable;

/**
 * Batch source to read from a BigQuery table
 */
@co.cask.cdap.api.annotation.Plugin(type = "batchsource")
@Name("BigQuerySource")
@Description("Reads from a database table(s) using a configurable BigQuery." +
  " Outputs one record for each row returned by the query.")
public class BigQuerySource extends ReferenceBatchSource<LongWritable, JsonObject, StructuredRecord> {
  private final BQSourceConfig sourceConfig;

  public BigQuerySource(BQSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.sourceConfig = config;
  }
  @Override
  public void prepareRun(BatchSourceContext context) {

  }

  public static class BQSourceConfig extends PluginConfig {
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

  }
}
