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
package com.vrudenskyi.kafka.azure.storage.blob;

import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.vrudenskyi.kafka.azure.storage.AzureStorageWriter;
import com.vrudenskyi.kafka.azure.storage.ConfigValidators;
import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSinkConnector;

/**
 * Base writer for Azure Blobs.
 * It  manages CloudBlob Instansence(s) for each topic-partition
 *  * it also responsible for blobs rotation.
 *  
 *  it is not responsible for format 
 * 
 * @author ew4ahmz
 *
 */
public abstract class CloudBlobStorageWriter extends AzureStorageWriter {

  protected static final Logger log = LoggerFactory.getLogger(CloudBlobStorageWriter.class);

  /**
   * caches blob refrerences  for TopicPartitions
   */
  private LoadingCache<TopicPartition, CloudAppendBlob> blobsCache;

  private static final Pattern BLOB_ROLLING_REGEX = Pattern.compile(".*\\_(\\d+)$");

  /**
   * Configs
   */
  private static final String CONFIG_PREFIX = "blob.";

  public static final String BLOB_NAME_PATTERN_CONFIG = CONFIG_PREFIX + "namePattern";
  private static final String BLOB_NAME_PATTERN_DEFAULT = "{topic}/{timestamp,yyyy}/{timestamp,MM}/{timestamp,dd}/{partition}.data";

  public static final String BLOB_ROLL_CHECK_INTERVAL_CONFIG = CONFIG_PREFIX + "rollCheckInterval";
  private static final int BLOB_ROLL_INTERVAL_DEFAULT = 5;

  public static final String BLOB_BLOCK_COUNT_CONFIG = CONFIG_PREFIX + "maxCommittedBlockCount";
  private static final int BLOB_BLOCK_COUNT_DEFAULT = 49990;

  public static final String CONTAINER_NAME_CONFIG = CONFIG_PREFIX + "containerName";

  public static final ConfigDef BLOB_STORAGE_CONFIG_DEF = new ConfigDef().define(BLOB_NAME_PATTERN_CONFIG, ConfigDef.Type.STRING, BLOB_NAME_PATTERN_DEFAULT, null, ConfigDef.Importance.HIGH, "Blobname pattern")
      .define(CONTAINER_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigValidators.CONTAINER_NAME_VALIDATOR, ConfigDef.Importance.HIGH, "Container name for blob")
      .define(BLOB_ROLL_CHECK_INTERVAL_CONFIG, ConfigDef.Type.INT, BLOB_ROLL_INTERVAL_DEFAULT, null, ConfigDef.Importance.LOW, "Blob roll interval in minutes. Default: 5 minutes.")
      .define(BLOB_BLOCK_COUNT_CONFIG, ConfigDef.Type.INT, BLOB_BLOCK_COUNT_DEFAULT, null, ConfigDef.Importance.LOW, "Max blocks per blob");

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    final SimpleConfig config = new SimpleConfig(BLOB_STORAGE_CONFIG_DEF, sinkConfig.originalsWithPrefix(AzureStorageSinkConnector.AZURE_SINK_WRITER_CONFIG + "."));
    final String blobIdPattern = config.getString(BLOB_NAME_PATTERN_CONFIG);
    final int expireMinutes = config.getInt(BLOB_ROLL_CHECK_INTERVAL_CONFIG);
    final int maxBlocks = config.getInt(BLOB_BLOCK_COUNT_CONFIG);
    String storageContainer = config.getString(CONTAINER_NAME_CONFIG);
    try {
      CloudBlobClient serviceClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer container = serviceClient.getContainerReference(storageContainer);
      container.createIfNotExists();

      log.debug("create blobs cache");
      blobsCache = CacheBuilder.newBuilder().concurrencyLevel(4).maximumSize(1000).expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
          /**
          .removalListener(new RemovalListener<TopicPartition, CloudAppendBlob>() {
            @Override
            public void onRemoval(RemovalNotification<TopicPartition, CloudAppendBlob> notification) {
              log.debug("Removed ");
            }
          })
          **/
          .build(new CacheLoader<TopicPartition, CloudAppendBlob>() {
            public CloudAppendBlob load(TopicPartition tp) throws Exception {
              String baseBlobId = MessageFormat.format(blobIdPattern.replaceAll("\\{topic\\}", "{0}").replaceAll("\\{partition\\}", "{1}").replaceAll("\\{timestamp", "{2,date"), tp.topic(), tp.partition(), new Date());
              String blobId = baseBlobId;
              boolean created = false;
              CloudAppendBlob blob;
              do {
                blob = container.getAppendBlobReference(blobId);
                if (!blob.exists()) {
                  blob.createOrReplace();
                  onBlobCreate(blob);
                  created = true;
                } else {
                  if (blob.getProperties().getAppendBlobCommittedBlockCount() >= maxBlocks) {
                    log.info("Blob {} exists and and has CommittedBlocks more then {}. Rolling...", blob.getName(), maxBlocks);
                    if (BLOB_ROLLING_REGEX.matcher(blobId).matches()) {
                      String suffix = BLOB_ROLLING_REGEX.matcher(blobId).replaceAll("$1");
                      int idx = Integer.parseInt(suffix);
                      idx++;
                      blobId = baseBlobId + "_" + idx;
                    } else {
                      blobId = baseBlobId + "_1";
                    }
                  } else {
                    created = true;
                  }
                }
              } while (!created);

              return blob;
            }
          });
    } catch (Exception e) {
      throw new ConfigException("Failed to configure CloudBlobStorageWriter", e);
    }

  }

  /**
   * Called  on blob creation 
   * @param blob
   */
  protected void onBlobCreate(CloudAppendBlob blob) {
  }

  protected CloudAppendBlob getBlob(TopicPartition tp) throws ConnectException {
    try {
      return blobsCache.get(tp);
    } catch (ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  protected void invalidateBlob(TopicPartition tp) throws ConnectException {
    blobsCache.invalidate(tp);
  }

  @Override
  public void close() {
    super.close();
    blobsCache.cleanUp();
  }

}
