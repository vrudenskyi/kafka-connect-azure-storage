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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.blob.CloudBlob;
import com.vrudenskyi.kafka.azure.storage.AzureBlobSeekableInput;
import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSourceConnector;

/**
 * Implementation of Azure blob reader in avro format
 * 
 * @author ew4ahmz
 */
public class AvroBlobReader extends CloudBlobStorageReader {

  private static final Logger log = LoggerFactory.getLogger(AvroBlobReader.class);

  /** Key name  for SourceRecord partition, value equals to blob name   */
  public static final String PARTITION_KEY_NAME = "avro.blob.partition.name";

  /** Configs for avro reader  */

  //optimized read, will keep reading until next avro sync
  //will increase performance by reducing number of re-reads/skips
  //but may exceed configured batch size
  public static final String AVRO_OPTIMIZED_READ_CONFIG = "optimizedRead";
  public static final Boolean AVRO_SMART_READ_DEFAULT = Boolean.TRUE;
  private static final ConfigDef AVRO_READER_CONFIG_DEF = new ConfigDef().define(AVRO_OPTIMIZED_READ_CONFIG, ConfigDef.Type.BOOLEAN, AVRO_SMART_READ_DEFAULT, ConfigDef.Importance.MEDIUM,
      "If enabled read will be optimized to reduce number of re-reads. default: true");

  /** inner state **/
  //private CloudBlob blob;
  private AvroReaderOffset offset;
  private Map<String, String> partition;
  private boolean optimizedRead = true;
  private DataFileReader<Object> reader;

  /**
   * Avro reader specific configs
   */
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    final SimpleConfig conf = new SimpleConfig(AVRO_READER_CONFIG_DEF, sourceConfig.originalsWithPrefix(AzureStorageSourceConnector.AZURE_SOURCE_READER_CONFIG + "."));
    this.optimizedRead = conf.getBoolean(AVRO_OPTIMIZED_READ_CONFIG);
  }

  @Override
  public boolean read(int maxRecords, List<SourceRecord> resultList, OffsetStorageReader offsetStorageReader) throws Exception {

    LinkedList<CloudBlob> blobs = getBlobs();
    //initialize state for new blob if needed
    if (reader == null && !blobs.isEmpty()) {
      CloudBlob blob = blobs.pop();
      this.partition = Collections.singletonMap(PARTITION_KEY_NAME, blob.getName());
      offset = AvroReaderOffset.newInstance(offsetStorageReader.offset(partition));
      log.debug("Initialize reader for blob: {}, currentOffset: {}", blob.getName(), offset);
      reader = new DataFileReader<>(new AzureBlobSeekableInput(blob), new GenericDatumReader<>());
      offset.syncReader(reader);
    }
    if (reader == null) {
      return false;
    }
    int readCounter = 0;
    Object obj = null;
    boolean done = !reader.hasNext() || readCounter >= this.batchSize;
    while (!done) {
      readCounter++;
      obj = reader.next();
      resultList.add(toSourceRecord(obj));
      offset.incrementShift();
      boolean synced = offset.updFromReader(reader);

      done = !reader.hasNext() || readCounter >= batchSize;
      //for 'smart reading' always read till sync. to avoid re-reads in the future (maintain 'shift' always 0)  
      if (this.optimizedRead) {
        done = done && synced;
      }
    }
    log.debug("Optimized read: {}, batch size: {},  effectively read: {}, current offset: {}", this.optimizedRead, this.batchSize, readCounter, this.offset);
    boolean blobHasNext = reader.hasNext();
    if (!blobHasNext) {
      reader.close();
      log.debug("reader cloase for partition: {}", this.partition);
      reader = null;
    }
    return blobHasNext || !blobs.isEmpty();
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        log.warn("Failed to close AvroReader", e);
      }
    }
  }

  private SourceRecord toSourceRecord(Object obj) {
    Record rec = (Record) obj;

    ByteBuffer keybb = (ByteBuffer) rec.get("key");
    byte[] keyData = keybb == null ? null : keybb.array();
    SchemaAndValue keySV = keyConverter.toConnectData(topic, keyData);

    ByteBuffer valuebb = (ByteBuffer) rec.get("value");
    byte[] valueData = valuebb == null ? null : valuebb.array();
    SchemaAndValue valueSV = valueConverter.toConnectData(topic, valueData);

    SourceRecord sRec = new SourceRecord(this.partition, this.offset.asMap(), this.topic, keySV.schema(), keySV.value(), valueSV.schema(), valueSV.value());

    Map<Utf8, List<ByteBuffer>> headers = (Map<Utf8, List<ByteBuffer>>) rec.get("headers");
    if (headers != null && headers.size() > 0) {
      for (Map.Entry<Utf8, List<ByteBuffer>> hdrE : headers.entrySet()) {
        for (ByteBuffer hdrBb : hdrE.getValue()) {
          SchemaAndValue hdrValue = null;
          try {
            hdrValue = headersConverter.toConnectHeader(this.topic, hdrE.getKey().toString(), hdrBb.array());
          } catch (Exception e) {
            hdrValue = new SchemaAndValue(Schema.STRING_SCHEMA, "" + hdrBb);
            sRec.headers().addBoolean("blobRestoreError", true);
            sRec.headers().addString("blobRestoreHeaderName", hdrE.getKey().toString());
            sRec.headers().addString("blobRestoreErrorMessage", e.getMessage());
          }
          sRec.headers().add(hdrE.getKey().toString(), hdrValue);
        }
      }
    }
    return sRec;
  }

}
