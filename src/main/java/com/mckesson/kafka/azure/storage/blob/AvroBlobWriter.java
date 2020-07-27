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
package com.mckesson.kafka.azure.storage.blob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.mckesson.kafka.azure.storage.AzureBlobSeekableInput;
import com.mckesson.kafka.connect.azure.storage.AzureStorageSinkConnector;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudAppendBlob;

/**
 * CloudBlobStorageWriter implementation  to write data in Avro format
 */
public class AvroBlobWriter extends CloudBlobStorageWriter {

  private static final String CONFIG_PREFIX = "avro.";

  public static final String AVRO_SYNC_INTERVAL_CONFIG = CONFIG_PREFIX + "syncInterval";
  private static final Integer AVRO_SYNC_INTERVAL_DEFAULT = 1024 * 1024; //1M

  public static final String AVRO_FLUSH_ON_EVERY_BLOCK_CONFIG = CONFIG_PREFIX + "flushOnEveryBlock";
  private static final Boolean AVRO_FLUSH_ON_EVERY_BLOCK_DEFAULT = Boolean.TRUE;

  public static final String AVRO_CODEC_CONFIG = CONFIG_PREFIX + "codec";
  private static final String AVRO_CODEC_DEFAULT = "null";

  private static final ConfigDef AVRO_WRITER_CONFIG_DEF = new ConfigDef()
      .define(AVRO_SYNC_INTERVAL_CONFIG, ConfigDef.Type.INT, AVRO_SYNC_INTERVAL_DEFAULT, ConfigDef.Importance.MEDIUM,
          "Set the synchronization interval for this file, in bytes.  Valid values range from 32 to 2^30  Suggested values are between 2K and 2M. default 16k")
      .define(AVRO_FLUSH_ON_EVERY_BLOCK_CONFIG, ConfigDef.Type.BOOLEAN, AVRO_FLUSH_ON_EVERY_BLOCK_DEFAULT, ConfigDef.Importance.MEDIUM,
          "Set whether this writer should flush the block to the stream every time a sync marker is written. default:true")
      .define(AVRO_CODEC_CONFIG, ConfigDef.Type.STRING, AVRO_CODEC_DEFAULT, ConfigDef.Importance.MEDIUM, "Avro codec name. supported: deflate, snappy, bzip2, xz.  default:null");

  /**
   * Default schema:
   *  byte[] key
   *  byte[] value
   *  map<List<byte[]>> headers
   */
  public static final Schema MCK_ARCHIVE_SCHEMA = SchemaBuilder.record("mck.archive").fields()
      .name("key").type().nullable().bytesType().noDefault()
      .name("value").type().nullable().bytesType().noDefault()
      .name("headers").type().nullable().map().values().array().items().nullable().bytesType().mapDefault(null)
      .endRecord();
  private Schema schema = MCK_ARCHIVE_SCHEMA;
  private int syncInterval = AVRO_SYNC_INTERVAL_DEFAULT;
  private boolean flushOnEveryBlock = AVRO_FLUSH_ON_EVERY_BLOCK_DEFAULT;
  private String codecName = AVRO_CODEC_DEFAULT;

  private Map<TopicPartition, Entry<String, DataFileWriter<Object>>> writers = new HashMap<>();

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    //add specific things
    final SimpleConfig config = new SimpleConfig(AVRO_WRITER_CONFIG_DEF, sinkConfig.originalsWithPrefix(AzureStorageSinkConnector.AZURE_SINK_WRITER_CONFIG + "."));
    flushOnEveryBlock = config.getBoolean(AVRO_FLUSH_ON_EVERY_BLOCK_CONFIG);
    syncInterval = config.getInt(AVRO_SYNC_INTERVAL_CONFIG);
    this.codecName = config.getString(AVRO_CODEC_CONFIG);
  }

  @Override
  public void write(TopicPartition tp, byte[] key, byte[] value, Map<String, List<byte[]>> headers) throws ConnectException {
    /// before write make sure that blob was not changed due-to blob rotation

    CloudAppendBlob currentBlob = getBlob(tp);
    String currentName = currentBlob.getName();
    Entry<String, DataFileWriter<Object>> tpWriter = writers.get(tp);

    if (tpWriter == null) {
      log.debug("AFTER_ERROR for: {}", tp);
      if (assignments.contains(tp)) {
        log.info("Assigned partition does not have a writer. Writer will be re-created for: {}", tp);
        try {
          //open writer
          CloudAppendBlob blob = getBlob(tp);
          tpWriter = new AbstractMap.SimpleEntry<>(blob.getName(), loadOrCreateAvroWriter(blob));
          writers.put(tp, tpWriter);
        } catch (Exception e) {
          throw new ConnectException("Failed to create writer for assigned partition: " + tp, e);
        }
      } else {
        throw new ConnectException("Data for not opened partition passed for write: " + tp);
      }

    }

    //current blobName  different from assosiated: close-flush and re-create
    if (!currentName.equals(tpWriter.getKey())) {
      log.info("Blob name changed for {}  from: {}, to: {}", tp, tpWriter.getKey(), currentName);
      try {
        tpWriter.getValue().close();
        tpWriter = new AbstractMap.SimpleEntry<>(currentBlob.getName(), loadOrCreateAvroWriter(currentBlob));
        writers.replace(tp, tpWriter);
      } catch (Exception e) {
        throw new ConnectException("Failed to re-create AvroWriter on changed blob name for: " + tp, e);
      }
    }

    GenericRecordBuilder recBuilder = new GenericRecordBuilder(schema)
        .set("key", key == null ? null : ByteBuffer.wrap(key))
        .set("value", value == null ? null : ByteBuffer.wrap(value));

    if (headers != null && headers.size() > 0) {
      Map<String, List<ByteBuffer>> hdrBufs = new LinkedHashMap<>(headers.size());
      headers.forEach((hk, hv) -> {
        hdrBufs.putIfAbsent(hk, new ArrayList<>());
        for (byte[] hbytes : hv) {
          hdrBufs.get(hk).add((hbytes == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(hbytes)));
        }
      });
      recBuilder.set("headers", hdrBufs);
    }

    Record rec = recBuilder.build();
    try {
      tpWriter.getValue().append(rec);
    } catch (IOException e) {
      throw translateException(tp, "Failed to append avro record for: " + tp, e);
    }

  }

  private ConnectException translateException(TopicPartition tp, String message, IOException e) {

    Throwable cause = e.getCause();
    if (cause instanceof StorageException) {
      StorageException se = (StorageException) cause;
      log.error("StorageException: errCode: {}, httpStatus: {},  extInfo: {} ", se.getErrorCode(), se.getHttpStatusCode(), se.getExtendedErrorInformation(), se);
      try {
        log.debug("Remove cached writer/blob before throwing RetriableException for: {} ", tp);
        writers.remove(tp);
        invalidateBlob(tp);
      } catch (Exception finalEx) {
        throw new ConnectException("Failed on re-init writer for:" + tp, finalEx);
      }
      return new RetriableException(message, cause);
    }
    return new ConnectException(message, e);
  }

  @Override
  public void flush(TopicPartition tp) throws ConnectException {
    if (writers.containsKey(tp)) {
      try {
        writers.get(tp).getValue().flush();
      } catch (IOException e) {
        throw translateException(tp, "Failed to flush AvroDataFileWriter for: " + tp, e);
      }
    } else {
      if (assignments.contains(tp)) {
        log.warn("Incosistant state while flushing: assigned partition does not have a writer. Partition: {}", tp);
      } else {
        log.warn("Flush for not opened partition passed");
      }
    }
  }

  @Override
  public void openPartition(TopicPartition tp) throws ConnectException {
    CloudAppendBlob blob = getBlob(tp);
    String blobName = blob.getName();

    DataFileWriter<Object> avroWriter;
    try {
      avroWriter = loadOrCreateAvroWriter(blob);
    } catch (Exception e) {
      throw new ConnectException("Failed to create Avro DataFileWriter for: " + tp, e);
    }
    writers.put(tp, new AbstractMap.SimpleEntry<>(blobName, avroWriter));
  }

  @Override
  public void closePartition(TopicPartition tp) throws ConnectException {
    flush(tp);
    if (writers.containsKey(tp)) {
      try {
        writers.get(tp).getValue().close();
      } catch (IOException e) {
        throw new ConnectException("Failed to close AvroDataFileWriter for: " + tp, e);
      }
    }
    writers.remove(tp);

  }

  private DataFileWriter<Object> loadOrCreateAvroWriter(CloudAppendBlob blob) throws Exception {
    DataFileWriter<Object> avroWriter = new DataFileWriter<>(new GenericDatumWriter<>());
    avroWriter.setSyncInterval(syncInterval);
    avroWriter.setFlushOnEveryBlock(flushOnEveryBlock);
    avroWriter.setCodec(CodecFactory.fromString(codecName));

    // check if already exists
    if (blob.exists() && blob.getProperties().getLength() > 16) {
      log.info("AvroWriter create: APPEND to Blob {}", blob.getName());
      AzureBlobSeekableInput absi = new AzureBlobSeekableInput(blob);
      BlobOutputStream bos = blob.openWriteExisting();
      avroWriter.appendTo(absi, bos); ///!! existing
    } else {
      // create new writer;
      log.info("AvroWriter create: NEW Blob {}", blob.getName());
      blob.createOrReplace();
      BlobOutputStream bos = blob.openWriteExisting();
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] sync = md5.digest(blob.getName().getBytes());
      avroWriter.create(schema, bos, sync); ///!! new
    }
    return avroWriter;

  }

}
