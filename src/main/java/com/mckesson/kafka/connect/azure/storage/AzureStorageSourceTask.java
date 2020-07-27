/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mckesson.kafka.connect.azure.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.azure.storage.AzureStorageReader;

public class AzureStorageSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(AzureStorageSourceTask.class);

  private AzureStorageReader storageReader;
  private String topic;

  private long lastPollTime = 0L;
  private Boolean keepPolling;
  private long pollInterval;
  private int batchSizeEvents;
  private AtomicBoolean stop;
  private boolean dataAvailable = true;

  @Override
  public String version() {
    return Version.version;
  }

  @Override
  public void start(Map<String, String> props) {
    AbstractConfig config = new SimpleConfig(AzureStorageSourceConnector.CONFIG_DEF, props);

    this.topic = config.getString(AzureStorageSourceConnector.TOPIC_CONFIG);
    this.storageReader = config.getConfiguredInstance(AzureStorageSourceConnector.AZURE_SOURCE_READER_CONFIG, AzureStorageReader.class);
    this.batchSizeEvents = config.getInt(AzureStorageSourceConnector.AZURE_SOURCE_BATCH_SIZE_EVENTS_CONFIG);
    this.pollInterval = config.getInt(AzureStorageSourceConnector.AZURE_SOURCE_POLL_INTERVAL_CONFIG) * 1000L;
    this.keepPolling = config.getBoolean(AzureStorageSourceConnector.AZURE_SOURCE_POLL_CONFIG);
    this.stop = new AtomicBoolean(false);
    log.info("Source for topic {} successfully started", this.topic);
  }

  @Override
  public void stop() {
    stop.set(true);
    storageReader.close();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("Start polling for new data...");
    List<SourceRecord> result = new ArrayList<>(batchSizeEvents);
    if (!dataAvailable && !keepPolling)
      throw new ConnectException("NO_MORE_DATA_AVAILABLE");
    if (stop.get())
      return Collections.emptyList();

    while (true) {
      try {
        dataAvailable = storageReader.read(batchSizeEvents, result, context.offsetStorageReader());
      } catch (Exception e) {
        throw new ConnectException("Failed to read data from storage", e);
      }
      if (result.size() >= batchSizeEvents)
        break; //enough data collected

      /////TODO review logic to make it more clear!!
      //wait if no data available
      if (!dataAvailable && keepPolling) {
        boolean polltime = (lastPollTime + pollInterval) < System.currentTimeMillis();
        if (polltime && result.size() > 0)
          break; //time to return any data
        Thread.sleep(1000L);
        if (stop.get())
          break; //exit on stop signal
      } else {
        break;
      }
    }
    lastPollTime = System.currentTimeMillis();
    return result;

  }
}
