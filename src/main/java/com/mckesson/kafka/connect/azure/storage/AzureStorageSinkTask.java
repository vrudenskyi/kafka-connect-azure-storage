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

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.azure.storage.AzureStorageWriter;

public class AzureStorageSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(AzureStorageSinkTask.class);

  AzureStorageWriter writer;

  @Override
  public String version() {
    return Version.version;
  }

  @Override
  public void start(Map<String, String> props) {
    
    AbstractConfig config = new SimpleConfig(AzureStorageSinkConnector.CONFIG_DEF, props);
    writer = config.getConfiguredInstance(AzureStorageSinkConnector.AZURE_SINK_WRITER_CONFIG, AzureStorageWriter.class);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.info("Stopping task, closing all writers: {}", partitions);
    writer.closePartitions(partitions);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    writer.openPartitions(partitions);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records == null || records.isEmpty()) {
      return;
    }

    if (log.isTraceEnabled()) {
      SinkRecord first = records.iterator().next();
      log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the azure...", records.size(), first.topic(), first.kafkaPartition(), first.kafkaOffset());
    }
    writer.write(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    for (TopicPartition tp : currentOffsets.keySet()) {
      writer.flush(tp);
      log.debug("Flushed data to Azure with following offsets: {}", currentOffsets);
    }
  }

  @Override
  public void stop() {
    if (writer != null) {
      writer.close();
    }
  }

}
