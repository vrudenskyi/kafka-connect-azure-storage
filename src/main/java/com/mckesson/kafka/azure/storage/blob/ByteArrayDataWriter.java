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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.microsoft.azure.storage.blob.CloudAppendBlob;

public class ByteArrayDataWriter extends CloudBlobStorageWriter {

  private final Map<TopicPartition, ByteArrayDataOutput> blobBuffers = new HashMap<>();
  public static final int UNIT_SEPARATOR = 31; // ASCII 31 Unit Separator
  public static final int RECORD_SEPARATOR = 30; // ASCII 30 Record Separator

  @Override
  public void write(TopicPartition tp, byte[] key, byte[] value, Map<String, List<byte[]>> headers) throws ConnectException {

    ByteArrayDataOutput bb = getBlobBuffer(tp);
    
    byte[] keyData = key == null ? new byte[0] : key;
    bb.write(keyData);
    bb.write(UNIT_SEPARATOR); // ASCII 31 Unit Separator
    getBytesCounter(tp).addAndGet(keyData.length + 1);

    byte[] valueData = value == null ? new byte[0] : value;
    bb.write(valueData);
    bb.write(RECORD_SEPARATOR); // ASCII 30 Record Separator
    getBytesCounter(tp).addAndGet(valueData.length + 1);

  }

  private ByteArrayDataOutput getBlobBuffer(TopicPartition tp) {
    if (!blobBuffers.containsKey(tp)) {
      blobBuffers.put(tp, ByteStreams.newDataOutput());
    }
    return blobBuffers.get(tp);
  }

  @Override
  public void openPartition(TopicPartition tp) throws ConnectException {
    blobBuffers.put(tp, ByteStreams.newDataOutput());
  }

  @Override
  public void closePartition(TopicPartition tp) throws ConnectException {
    flush(tp);
    blobBuffers.remove(tp);

  }

  @Override
  public void flush(TopicPartition tp) throws ConnectException {
    CloudAppendBlob blob = getBlob(tp);
    ByteArrayDataOutput data = getBlobBuffer(tp);
    if (blob != null && data != null) {

      try {
        byte[] buffer = data.toByteArray();
        if (buffer.length > 0) {
          blob.appendFromByteArray(buffer, 0, buffer.length);
        }
      } catch (Exception e) {
        throw new ConnectException("Failed to append data to blob", e);
      }

    }
    blobBuffers.replace(tp, ByteStreams.newDataOutput());
    resetCounters(tp);

  }

}
