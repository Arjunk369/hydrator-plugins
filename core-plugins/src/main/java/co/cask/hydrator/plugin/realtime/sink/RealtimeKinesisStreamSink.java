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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.hydrator.plugin.common.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static co.cask.hydrator.plugin.common.KinesisUtil.createAndWaitForStreamToBecomeAvailable;

/**
 * A {@link RealtimeSink} that writes data to a Amazon Kinesis stream.
 * If Kinesis Stream does not exist, it will be created using properties provided with this sink.
 */
@Plugin(type = RealtimeSink.PLUGIN_TYPE)
@Name("KinesisSink")
@Description("Real-time sink that outputs to a specified AWS Kinesis stream.")
public class RealtimeKinesisStreamSink extends RealtimeSink<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(RealtimeKinesisStreamSink.class);
  private final KinesisConfig config;
  private static AmazonKinesisClient kinesisClient;


  public RealtimeKinesisStreamSink(KinesisConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.name),
                                "Stream name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessKey),
                                "Access Key required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessSecret),
                                "Access Key secret required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.partition),
                                "partition name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.bodyField),
                                "Please map the input field containing data");

  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    BasicAWSCredentials awsCred = new BasicAWSCredentials(config.awsAccessKey, config.awsAccessSecret);
    kinesisClient = new AmazonKinesisClient(awsCred);
    createAndWaitForStreamToBecomeAvailable(kinesisClient,
                                            config.name,
                                            config.shardCount);
  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;
    for (StructuredRecord structuredRecord : structuredRecords) {
      Schema schema = structuredRecord.getSchema();
      Object data = structuredRecord.get(config.bodyField);
      if (data == null) {
        LOG.debug("Found null data. Skipping record.");
        continue;
      }
      Schema.Field dataSchemaField = schema.getField(config.bodyField);
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(config.name);
      putRecordRequest.setPartitionKey(config.partition);
      switch (dataSchemaField.getSchema().getType()) {
        case BYTES:
          numRecordsWritten += writeBytes(putRecordRequest, data);
          break;
        case STRING:
          numRecordsWritten += writeString(putRecordRequest, data);
          break;
        default:
          if (dataSchemaField.getSchema().getType().isSimpleType()) {
            numRecordsWritten += writeBytes(putRecordRequest, String.valueOf(data));
            break;
          }
          LOG.debug("Type {} is not supported for writing to stream", data.getClass().getName());
          break;
      }
    }
    return numRecordsWritten;
  }

  private int writeString(PutRecordRequest putRecordRequest, Object data) {
    putRecordRequest.setData(ByteBuffer.wrap(Bytes.toBytes((String) data)));
    try {
      PutRecordResult result = kinesisClient.putRecord(putRecordRequest);
      LOG.info("Data written at {} ", result.toString());
      return 1;
    } catch (Exception e) {
      LOG.debug("could not write data to stream {}", putRecordRequest.getStreamName(), e);
      e.printStackTrace();
      return  0;
    }
  }

  private int writeBytes(PutRecordRequest putRecordRequest, Object data) {
    ByteBuffer buffer;
    if (data instanceof ByteBuffer) {
      buffer = (ByteBuffer) data;
    } else if (data instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) data);
    } else {
      LOG.debug("Type {} is not supported for writing to stream", data.getClass().getName());
      return 0;
    }
    putRecordRequest.setData(buffer);
    try {
      PutRecordResult result = kinesisClient.putRecord(putRecordRequest);
      LOG.info("Data written at {} ", result.toString());
      return 1;
    } catch (Exception e) {
      LOG.debug("could not write data to stream {}", putRecordRequest.getStreamName(), e);
      return  0;
    }
  }

  public KinesisConfig getConfig() {
    return config;
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisConfig extends PluginConfig {

    @Description("The name of the Kinesis stream to output to. Must be a valid Kinesis stream name." +
      " The Kinesis stream will be created if it does not exist.")
    private String name;

    @Name(Properties.KinesisRealtimeSink.BODY_FIELD)
    @Description("Name of the field in the record that contains the data to be " +
      "written to the specified stream. The data could be in binary format as a byte array or a ByteBuffer. " +
      "It can also be a String. If unspecified, the 'body' key is used.")
    private String bodyField;

    @Name(Properties.KinesisRealtimeSink.ACCESS_ID)
    @Description("AWS access Id having access to Kinesis streams")
    private String awsAccessKey;

    @Name(Properties.KinesisRealtimeSink.ACCESS_KEY)
    @Description("AWS access key secret having access to Kinesis streams")
    private String awsAccessSecret;

    @Name(Properties.KinesisRealtimeSink.SHARD_COUNT)
    @Description("Number of shards to be created, each shard has input of 1mb/s")
    private int shardCount;

    @Name(Properties.KinesisRealtimeSink.PARTITION_KEY)
    @Description("Partition key to identify shard")
    private String partition;

    KinesisConfig(String name, String bodyField, String awsAccessKey,
                  String awsAccessSecret,  String partition, int shardCount) {
      this.name = name;
      this.bodyField = bodyField;
      this.awsAccessKey = awsAccessKey;
      this.awsAccessSecret = awsAccessSecret;
      this.partition = partition;
      this.shardCount = shardCount;
    }
  }
}
