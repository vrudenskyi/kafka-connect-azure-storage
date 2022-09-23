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
package com.vrudenskyi.kafka.azure.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vrudenskyi.kafka.azure.storage.blob.CloudBlobStorageWriter;
import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSinkConnector;

public abstract class AzureStorageWriter extends AzureStorage {

  private static final Logger log = LoggerFactory.getLogger(CloudBlobStorageWriter.class);

  protected AbstractConfig sinkConfig;

  protected long bulkSizeBytes;
  protected int bulkSizeEvents;

  protected final Set<TopicPartition> assignments = new HashSet<>();
  private final Map<TopicPartition, AtomicInteger> eventCounters = new HashMap<>();
  private final Map<TopicPartition, AtomicLong> bytesCounters = new HashMap<>();

  /**
   * Configure writer
   */
  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    this.sinkConfig = new SimpleConfig(AzureStorageSinkConnector.CONFIG_DEF, props);
    bulkSizeBytes = sinkConfig.getLong(AzureStorageSinkConnector.AZURE_SINK_BULK_SIZE_BYTES_CONFIG);
    bulkSizeEvents = sinkConfig.getInt(AzureStorageSinkConnector.AZURE_SINK_BULK_SIZE_EVENTS_CONFIG);
  }

  /**
   * Close writer
   */
  public void close() {
    closePartitions(assignments);
    assignments.clear();
    bytesCounters.clear();
    eventCounters.clear();
  }

  public void openPartitions(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      openPartition(tp);
      assignments.add(tp);
      bytesCounters.put(tp, new AtomicLong(0L));
      eventCounters.put(tp, new AtomicInteger(0));

    }
  }

  public void closePartitions(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      closePartition(tp);
      eventCounters.remove(tp);
      bytesCounters.remove(tp);
      assignments.remove(tp);
    }
  }

  protected AtomicInteger getEventsCounter(TopicPartition tp) {
    if (!eventCounters.containsKey(tp)) {
      eventCounters.put(tp, new AtomicInteger(0));
    }
    return eventCounters.get(tp);
  }

  protected AtomicLong getBytesCounter(TopicPartition tp) {
    if (!bytesCounters.containsKey(tp)) {
      bytesCounters.put(tp, new AtomicLong(0L));
    }
    return bytesCounters.get(tp);
  }

  public void resetCounters(TopicPartition tp) throws ConnectException {
    getEventsCounter(tp).set(0);
    getBytesCounter(tp).set(0L);
  }

  public void write(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord rec : records) {
      TopicPartition tp = new TopicPartition(rec.topic(), rec.kafkaPartition());
      byte[] keyData = null;
      byte[] valueData = null;
      Map<String, List<byte[]>> hdrsData = null;

      if (rec.key() != null) {
        keyData = keyConverter.fromConnectData(rec.topic(), rec.keySchema(), rec.key());
      }

      if (rec.value() != null) {
        valueData = valueConverter.fromConnectData(rec.topic(), rec.valueSchema(), rec.value());
      }

      if (!rec.headers().isEmpty()) {
        hdrsData = new LinkedHashMap<>(rec.headers().size());
        Iterator<Header> i = rec.headers().iterator();
        while (i.hasNext()) {
          Header h = i.next();
          byte[] hData = headersConverter.fromConnectHeader(rec.topic(), h.key(), h.schema(), h.value());
          hdrsData.putIfAbsent(h.key(), new ArrayList<byte[]>());
          hdrsData.get(h.key()).add(hData);
        }
      }

      //if (valueData != null) { //TODO: add config writeNulls = true|false
      write(tp, keyData, valueData, hdrsData);
      int keyLength = keyData == null ? 0 : keyData.length;
      int valueLength = valueData == null ? 0 : valueData.length;
      getEventsCounter(tp).incrementAndGet();
      getBytesCounter(tp).addAndGet(keyLength + valueLength);
      flushIfNeeded(tp);
      //}
    }

  }

  /**
   * Flushes topic-partition if needed based on bulkSize[Events|Bytes] settings
   *     
   * @param tp
   * @return
   *      true - if flush occurs
   *      false - no flush happen 
   */
  protected boolean flushIfNeeded(TopicPartition tp) {
    int events = getEventsCounter(tp).get();
    long bytes = getBytesCounter(tp).get();
    if (events >= bulkSizeEvents || bytes >= bulkSizeBytes) {
      try {
        log.debug("Flushing partition: {} on bulk limit reached events={}, bytes={}", tp, events, bytes);
        flush(tp);
        resetCounters(tp);
        return true;
      } catch (Exception e) {
        log.error("Failed to flush in write for tp: " + tp);
      }
    }
    return false;

  }

  public abstract void write(TopicPartition tp, byte[] key, byte[] value, Map<String, List<byte[]>> headers) throws ConnectException;

  public abstract void openPartition(TopicPartition tp) throws ConnectException;

  public abstract void closePartition(TopicPartition tp) throws ConnectException;

  public abstract void flush(TopicPartition tp) throws ConnectException;

}
