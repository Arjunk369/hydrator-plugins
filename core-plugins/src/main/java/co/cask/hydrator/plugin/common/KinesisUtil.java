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

package co.cask.hydrator.plugin.common;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to help create and manage Kinesis Streams
 */
public class KinesisUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisUtil.class);

  /**
   * Creates an Amazon Kinesis stream if it does not exist and waits for it to become available
   *
   * @param kinesisClient The {@link AmazonKinesisClient} with Amazon Kinesis read and write privileges
   * @param streamName The Amazon Kinesis stream name to create
   * @param shardCount The shard count to create the stream with
   * @throws IllegalStateException Invalid Amazon Kinesis stream state
   * @throws IllegalStateException Stream does not go active before the timeout
   */
  public static void createAndWaitForStreamToBecomeAvailable(AmazonKinesisClient kinesisClient,
                                                             String streamName,
                                                             int shardCount) {
    if (streamExists(kinesisClient, streamName)) {
      String state = streamState(kinesisClient, streamName);
      switch (state) {
        case "DELETING":
          long waitTime = System.currentTimeMillis() + 1000 * 120;
          while (System.currentTimeMillis() < waitTime && streamExists(kinesisClient, streamName)) {
            try {
              LOG.info("...Deleting Stream {} ...", streamName);
              Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
            }
          }
          if (streamExists(kinesisClient, streamName)) {
            LOG.error("KinesisUtils timed out waiting for stream {} to delete", streamName);
            throw new IllegalStateException(String.format("KinesisUtils timed out waiting for stream {} to delete",
                                                          streamName));
          }
          break;
        case "ACTIVE":
          LOG.info("Stream {} is ACTIVE", streamName);
          return;
        case "CREATING":
          break;
        case "UPDATING":
          LOG.info("Stream {} is UPDATING", streamName);
          return;
        default:
          throw new IllegalStateException("Illegal stream state: " + state);
      }
    } else {
      CreateStreamRequest createStreamRequest = new CreateStreamRequest();
      createStreamRequest.setStreamName(streamName);
      createStreamRequest.setShardCount(shardCount);
      kinesisClient.createStream(createStreamRequest);
      LOG.info("Stream {} created", streamName);
    }
    long waitTime = System.currentTimeMillis() + (10 * 60 * 1000);
    while (System.currentTimeMillis() < waitTime) {
      try {
        Thread.sleep(1000 * 10);
      } catch (InterruptedException e) {
        LOG.info("Waiting {} milli seconds for the stream to be created", waitTime - System.currentTimeMillis());
      }
      try {
        String streamStatus = streamState(kinesisClient, streamName);
        if (streamStatus.equals("ACTIVE")) {
          LOG.info("Stream {} is ACTIVE", streamName);
          return;
        }
      } catch (ResourceNotFoundException e) {
        throw new IllegalStateException(String.format("Stream {} never went active", streamName), e);
      }
    }
  }

  /**
   * Return the state of a Amazon Kinesis stream.
   *
   * @param kinesisClient The {@link AmazonKinesisClient} with Amazon Kinesis read privileges
   * @param streamName The Amazon Kinesis stream to get the state of
   * @return String representation of the Stream state
   */
  private static String streamState(AmazonKinesisClient kinesisClient, String streamName) {
    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    describeStreamRequest.setStreamName(streamName);
    try {
      return kinesisClient.describeStream(describeStreamRequest).getStreamDescription().getStreamStatus();
    } catch (AmazonServiceException e) {
      return null;
    }
  }

  /**
   * Helper method to determine if an Amazon Kinesis stream exists.
   *
   * @param kinesisClient The {@link AmazonKinesisClient} with Amazon Kinesis read privileges
   * @param streamName The Amazon Kinesis stream to check for
   * @return true if the Amazon Kinesis stream exists, otherwise return false
   */
  private static boolean streamExists(AmazonKinesisClient kinesisClient, String streamName) {
    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    describeStreamRequest.setStreamName(streamName);
    try {
      kinesisClient.describeStream(describeStreamRequest);
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }

}
