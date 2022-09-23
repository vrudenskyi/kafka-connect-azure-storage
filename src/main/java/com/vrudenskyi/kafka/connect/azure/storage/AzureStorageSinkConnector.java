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
package com.vrudenskyi.kafka.connect.azure.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vrudenskyi.kafka.azure.storage.AzureStorage;

public class AzureStorageSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(AzureStorageSinkConnector.class);

  public static final String AZURE_SINK_WRITER_CONFIG = "azure.sink.writer";

  public static final String AZURE_SINK_BULK_SIZE_BYTES_CONFIG = "azure.sink.bulkSizeBytes";
  public static final long AZURE_SA_BULK_SIZE_BYTES_DEFAULT = Long.MAX_VALUE;

  public static final String AZURE_SINK_BULK_SIZE_EVENTS_CONFIG = "azure.sink.bulkSizeEvents";
  public static final int AZURE_SINK_BULK_SIZE_EVENTS_DEFAULT = 4096;

  public static final ConfigDef CONFIG_DEF = new ConfigDef(AzureStorage.CONFIG_DEF)
      .define(AZURE_SINK_WRITER_CONFIG, Type.CLASS, null, Importance.HIGH, "Writer class")
      .define(AZURE_SINK_BULK_SIZE_BYTES_CONFIG, Type.LONG, AZURE_SA_BULK_SIZE_BYTES_DEFAULT, Importance.MEDIUM, "Bulk size in bytes")
      .define(AZURE_SINK_BULK_SIZE_EVENTS_CONFIG, Type.INT, AZURE_SINK_BULK_SIZE_EVENTS_DEFAULT, Importance.MEDIUM, "Bulk size in events count");

  private Map<String, String> configProps;

  @Override
  public String version() {
    return Version.version;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    configProps = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureStorageSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void stop() throws ConnectException {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    return super.validate(connectorConfigs);
  }
}
