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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.azure.storage.AzureStorage;

public class AzureStorageSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(AzureStorageSourceConnector.class);

  public static final String TOPIC_CONFIG = "topic";

  public static final String AZURE_SOURCE_READER_CONFIG = "azure.source.reader";

  public static final String AZURE_SOURCE_BATCH_SIZE_EVENTS_CONFIG = "azure.source.batchSizeEvents";
  public static final Integer AZURE_SOURCE_BATCH_SIZE_EVENTS_DEFAULT = 4096;

  public static final String AZURE_SOURCE_BATCH_SIZE_BYTES_CONFIG = "azure.source.batchSizeBytes";
  public static final Integer AZURE_SOURCE_BATCH_SIZE_BYTES_DEFAULT = 4096 * 1024; // 4M;

  public static final String AZURE_SOURCE_POLL_INTERVAL_CONFIG = "azure.source.poll.interval.seconds";
  public static final Integer AZURE_SOURCE_POLL_INTERVAL_DEFAULT = 300;
  public static final String AZURE_SOURCE_POLL_CONFIG = "azure.source.poll";
  public static final Boolean AZURE_SOURCE_POLL_DEFAULT = Boolean.FALSE;

  public static final ConfigDef CONFIG_DEF = new ConfigDef(AzureStorage.CONFIG_DEF)
      .define(TOPIC_CONFIG, Type.STRING, null, null, Importance.HIGH, "Kafka topic")
      .define(AZURE_SOURCE_READER_CONFIG, Type.CLASS, null, null, Importance.HIGH, "AzureStorageReader implementation")
      .define(AZURE_SOURCE_BATCH_SIZE_BYTES_CONFIG, Type.INT, AZURE_SOURCE_BATCH_SIZE_BYTES_DEFAULT, null, Importance.MEDIUM, "Target batch size in bytes")
      .define(AZURE_SOURCE_BATCH_SIZE_EVENTS_CONFIG, Type.INT, AZURE_SOURCE_BATCH_SIZE_EVENTS_DEFAULT, null, Importance.MEDIUM, "Target batch size in # of events")
      .define(AZURE_SOURCE_POLL_CONFIG, Type.BOOLEAN, AZURE_SOURCE_POLL_DEFAULT, null, Importance.MEDIUM, "Keep polling after read complete")
      .define(AZURE_SOURCE_POLL_INTERVAL_CONFIG, Type.INT, AZURE_SOURCE_POLL_INTERVAL_DEFAULT, null, Importance.MEDIUM, "Poll interval if " + AZURE_SOURCE_POLL_CONFIG + "=true");

  AbstractConfig config;

  @Override
  public String version() {
    return Version.version;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    //just to validate config
    config = new SimpleConfig(CONFIG_DEF, props);
  }

  @Override
  public void stop() {
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureStorageSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {

    if (maxTasks > 1) {
      throw new ConnectException("currently only one task is supported");
    }
    //TODO: implement multi-tasked readers support
    //approach: 1. implement method List<String> partitions()  in configured reader which will return list of available partitions depending on config (Blobs, tbales and such)
    //2. organize partitions depending on maxTasks 

    return Arrays.asList(config.originalsStrings());
  }

  /**
  private List<String> configureSourcesForBlob() throws InvalidKeyException, URISyntaxException, StorageException {
    String storageConnectionString = config.getString(CONNECTION_STRING_CONFIG);
    String containerName = null; //config.getString(CONTAINER_NAME_CONFIG);
    List<String> directoryNames = config.getList(AZURE_SA_BLOB_DIRECTORIES_CONFIG);
  
    CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
    CloudBlobClient serviceClient = account.createCloudBlobClient();
    CloudBlobContainer container = serviceClient.getContainerReference(containerName);
  
    List<String> sources = new ArrayList<>();
  
    for (String dirName : directoryNames) {
      CloudBlobDirectory dir = container.getDirectoryReference(dirName);
      getBlobSources(dir, configuredSources);
    }
    return sources;
  
  }
  
  private void getBlobSources(CloudBlobDirectory dir, List<String> blobs) throws StorageException, URISyntaxException {
    for (ListBlobItem bi : dir.listBlobs()) {
      if (bi instanceof CloudBlobDirectory) {
        getBlobSources((CloudBlobDirectory) bi, blobs);
      } else if (bi instanceof CloudBlob) {
        blobs.add(((CloudBlob) bi).getName());
      } else {
        throw new IllegalStateException("Unknown type of Blob");
      }
    }
  
  }
  
  
  
  private List<String> configureSourcesForTable() throws InvalidKeyException, URISyntaxException {
  
    String storageConnectionString = config.getString(CONNECTION_STRING_CONFIG);
  
    Set<String> tblsConfig = new HashSet<>(config.getList(AZURE_SA_TABLES_CONFIG));
    String accountName = config.getString(SA_NAME_CONFIG);
    if (StringUtils.isBlank(accountName)) {
      log.debug("Storage account name not specified. Trying to find from connection string");
      Map<String, String> settings = Utility.parseAccountString(storageConnectionString);
      accountName = settings.get("AccountName");
      configProps.put(SA_NAME_CONFIG, accountName);
      log.debug("Discovered account name: {}", accountName);
    }
    if (StringUtils.isBlank(accountName)) {
      throw new ConnectException("AccountName not found");
    }
  
    CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
  
    CloudTableClient tableClient = account.createCloudTableClient();
  
    Iterable<String> tblsAll = tableClient.listTables();
  
    Pattern wlPattern = null;
    String wl = config.getString(AZURE_SA_TABLES_WHITELIST_PATTERN_CONFIG);
    if (StringUtils.isNotBlank(wl)) {
      wlPattern = Pattern.compile(wl);
    }
  
    Pattern blPattern = null;
    String bl = config.getString(AZURE_SA_TABLES_BLACKLIST_PATTERN_CONFIG);
    if (StringUtils.isNotBlank(bl)) {
      blPattern = Pattern.compile(bl);
    }
  
    Set<String> tableSet = new HashSet<>();
    log.debug("Found tables for sa: {}", tblsAll);
    for (String tblName : tblsAll) {
      if (tblsConfig.remove(tblName) || (wlPattern != null && wlPattern.matcher(tblName).matches())) {
        tableSet.add(tblName);
        log.debug("Table candidate to read: {}", tblName);
      }
      if (blPattern != null && blPattern.matcher(tblName).matches()) {
        tableSet.remove(tblName);
        log.debug("Table blacklisted: {}", tblName);
      }
    }
    if (!tblsConfig.isEmpty()) {
      log.warn("Configuration contains some names not found in account: {}", tblsConfig);
    }
    if (tableSet.isEmpty()) {
      log.error("No tables to read. Connector will not be started");
      throw new ConnectException("No tables to read");
    }
    log.info("Tables configured for read: {}", tableSet);
    return new ArrayList<>(tableSet);
  }
  ****/
}
