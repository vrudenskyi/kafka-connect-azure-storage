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
package com.vrudenskyi.kafka.azure.storage.queue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import com.vrudenskyi.kafka.azure.storage.AzureStorageWriter;

public class CloudQueueStorageWriter extends AzureStorageWriter {

  @Override
  public void write(TopicPartition tp, byte[] key, byte[] value, Map<String, List<byte[]>> headers) throws ConnectException {
    // TODO Auto-generated method stub

  }

  @Override
  public void openPartition(TopicPartition tp) throws ConnectException {
    // TODO Auto-generated method stub

  }

  @Override
  public void closePartition(TopicPartition tp) throws ConnectException {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush(TopicPartition tp) throws ConnectException {
    // TODO Auto-generated method stub

  }

}
