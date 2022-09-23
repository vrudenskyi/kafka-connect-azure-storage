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

import java.net.URISyntaxException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.vrudenskyi.kafka.azure.storage.AzureStorageReader;
import com.vrudenskyi.kafka.azure.storage.ConfigValidators;
import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSourceConnector;

public abstract class CloudBlobStorageReader extends AzureStorageReader {
  private static final Logger log = LoggerFactory.getLogger(CloudBlobStorageReader.class);

  /** Configs for reader  */
  public static final String BLOB_DIRS_CONFIG = "blobDirs";
  public static final String CONTAINER_NAME_CONFIG = "containerName";

  private static final ConfigDef BLOB_READER_CONFIG_DEF = new ConfigDef()
      .define(BLOB_DIRS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "Blob directories to retrieve")
      .define(CONTAINER_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigValidators.CONTAINER_NAME_VALIDATOR, ConfigDef.Importance.HIGH, "Container name for blob");

  protected String containerName;
  protected List<String> blobDirs;
  protected Date maxModified = new Date(0);

  protected LinkedList<CloudBlob> blobs;

  /**
   * Configure reader
   */
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    final SimpleConfig conf = new SimpleConfig(BLOB_READER_CONFIG_DEF, sourceConfig.originalsWithPrefix(AzureStorageSourceConnector.AZURE_SOURCE_READER_CONFIG + "."));
    this.containerName = conf.getString(CONTAINER_NAME_CONFIG);
    this.blobDirs = conf.getList(BLOB_DIRS_CONFIG);
  }

  public LinkedList<CloudBlob> getBlobs() {
    if (blobs == null || blobs.size() == 0) {
      scanBlobs();
    }
    return blobs;
  }

  protected void scanBlobs() {
    log.debug("Scan cloud blobs fot date after {}", maxModified);
    try {
      CloudBlobClient serviceClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer container = serviceClient.getContainerReference(containerName);
      blobs = new LinkedList<>();
      for (String dirName : blobDirs) {
        CloudBlobDirectory dir = container.getDirectoryReference(dirName);
        maxModified = getBlobSources(dir, null, blobs);
      }
    } catch (Exception e) {
      throw new ConnectException("Failed to get list of blobs", e);
    }
  }

  /**
   * Get list of CloudBlob
   * 
   * @param dir - scan directory
   * @param dt - max lastModifiedDate 
   * @param blobs - collection of blobs
   * @return
   * @throws StorageException
   * @throws URISyntaxException
   */
  protected Date getBlobSources(CloudBlobDirectory dir, Date dt, List<CloudBlob> blobs) throws StorageException, URISyntaxException {
    Date maxDate = dt != null ? dt : new Date(0);
    Date scanDate = dt != null ? dt : new Date(0);
    for (ListBlobItem bi : dir.listBlobs()) {
      if (bi instanceof CloudBlobDirectory) {
        getBlobSources((CloudBlobDirectory) bi, dt, blobs);
      } else if (bi instanceof CloudBlob) {
        Date lastModified = ((CloudBlob) bi).getProperties().getLastModified();
        if (lastModified.after(maxDate)) {
          maxDate = lastModified;
        }
        if (lastModified.after(scanDate)) {
          blobs.add(((CloudBlob) bi));
        }
      } else {
        throw new IllegalStateException("Unknown type of Blob");
      }
    }
    return maxDate;
  }

}
