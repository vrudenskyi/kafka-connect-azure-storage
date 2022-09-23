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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSourceConnector;

public abstract class AzureStorageReader extends AzureStorage {

  protected AbstractConfig sourceConfig;
  protected String topic;
  protected int batchSize;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    this.sourceConfig = new SimpleConfig(AzureStorageSourceConnector.CONFIG_DEF, configs);
    this.topic = this.sourceConfig.getString(AzureStorageSourceConnector.TOPIC_CONFIG);
    this.batchSize = this.sourceConfig.getInt(AzureStorageSourceConnector.AZURE_SOURCE_BATCH_SIZE_EVENTS_CONFIG);
  }

  public abstract void close();

  /**
   * Reads data into list
   * 
   * @param batchSize - number of items to return
   * @param list -  collection to fill
   * @param offsetStorageReader 
   * @return true if more data available
   * @throws Exception 
   */
  public abstract boolean read(int batchSize, List<SourceRecord> list, OffsetStorageReader offsetStorageReader) throws Exception;

}
