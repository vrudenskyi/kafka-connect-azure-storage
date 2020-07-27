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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.blob.CloudBlob;

public class ByteArrayDataReader extends CloudBlobStorageReader {
  private static final Logger log = LoggerFactory.getLogger(ByteArrayDataReader.class);
  private boolean hasMoreData = false;

  private CloudBlob blob;
  private Map<String, Object> offset;
  private Map<String, String> partition;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }


  @Override
  public boolean read(int maxRecords, List<SourceRecord> records, OffsetStorageReader offsetStorageReader) {

    if (records == null)
      throw new IllegalArgumentException("List must not be null");
    
    LinkedList<CloudBlob> blobs = getBlobs();    
    if (blobs == null || blobs.size() == 0) {
      log.debug("No Blobs found nothing to read.");
      return false;
    }
    this.blob = blobs.pop();
    String offsetString = offset.getOrDefault(AzureStorageBlobOffset.OFFSET_KEY, "0").toString();
    log.debug("Read from CloudBlob: maxRecords: {}, offset:{}", maxRecords, offset);

    final PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos;
    try {
      pos = new PipedOutputStream(pis);
    } catch (IOException e1) {
      throw new ConnectException("Failed to pipe streams", e1);
    }

    final long savedPos = (offsetString == null || offsetString.length() == 0) ? 0L : Long.valueOf(offsetString);

    // check if saved pos == blob size
    long blobLength = blob.getProperties().getLength();
    if (savedPos + 1 >= blobLength) {
      log.debug("No more data. Blob length:{}, savedPos: {}", blobLength, savedPos);
      return false;
    }

    Thread readThread = new Thread(new Runnable() {
      public void run() {
        try {
          blob.downloadRange(savedPos, null, pos);
          pos.flush();
          pos.close();
          log.debug("Whole blob is read.");
        } catch (Exception e) {
          log.trace("Exception in reading thread", e);
        }
      }
    });
    readThread.setName("READ_BLOB: " + blob.getName());
    // start reading thread
    readThread.start();

    int b = 0;
    long currentPos = savedPos;
    ByteArrayOutputStream keyBuffer = new ByteArrayOutputStream();
    ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
    boolean keyData = true;
    long eventCounter = 0;

    try {
      while ((b = pis.read()) != -1) {
        currentPos++;
        if (b == ByteArrayDataWriter.RECORD_SEPARATOR) {
          offset.put(AzureStorageBlobOffset.OFFSET_KEY, "" + currentPos);
          records.add(extractRecord(keyBuffer, valueBuffer));
          keyBuffer.reset();
          valueBuffer.reset();
          keyData = true;
          // update offset

          eventCounter++;

        } else if (b == ByteArrayDataWriter.UNIT_SEPARATOR) {
          keyData = false;
        } else {
          if (keyData) {
            keyBuffer.write(b);
          } else {
            valueBuffer.write(b);
          }
        }

        if (eventCounter >= maxRecords) {
          // terminate reading
          pos.close();
          pis.close();
          readThread.interrupt();
          hasMoreData = true;
          break;
        }

      }

    } catch (Exception e) {
      throw new RetriableException("Read events failed", e);
    }

    return hasMoreData;
  }

  private SourceRecord extractRecord(ByteArrayOutputStream keyBuffer, ByteArrayOutputStream valueBuffer) {
    return new SourceRecord(this.partition, new HashMap<>(this.offset), topic, null, new String(keyBuffer.toByteArray()), null, new String(valueBuffer.toByteArray()));
  }

  @Override
  public void close() {
    blob = null;

  }

}
