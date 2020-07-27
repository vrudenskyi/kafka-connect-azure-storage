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
package com.mckesson.kafka.azure.storage;

import java.io.IOException;

import org.apache.avro.file.SeekableInput;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.CloudBlob;

public class AzureBlobSeekableInput implements SeekableInput {

  private CloudBlob blob;
  private BlobInputStream blobIS;
  private long blobPos = 0L;

  public AzureBlobSeekableInput(CloudBlob cloudBlob) throws StorageException {
    if (cloudBlob == null) {
      throw new NullPointerException("Cloud blob  is null");
    }
    this.blob = cloudBlob;
    this.blobIS = this.blob.openInputStream();
  }

  @Override
  public void close() throws IOException {
    if (this.blobIS != null) {
      this.blobIS.close();
    }

  }

  @Override
  public void seek(long p) throws IOException {
    //reset IS and re-position
    try {
      this.blobIS.close();
      this.blobIS = this.blob.openInputStream();
      this.blobIS.skip(p);
      this.blobPos = p;
    } catch (StorageException e) {
      throw new IOException(e);
    }

  }

  @Override
  public long tell() throws IOException {
    return this.blobPos;
  }

  @Override
  public long length() throws IOException {
    return blob.getProperties().getLength();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int result = this.blobIS.read(b, off, len);
    this.blobPos += result;
    return result;
  }

}
