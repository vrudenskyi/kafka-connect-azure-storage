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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileReader;

/**
 * Helper class to manage offset for  Avro DataFileReader
 * 
 * @author ew4ahmz
 *
 */
public class AvroReaderOffset {

  /*
   * position of the last occured sync
   */
  public static final String LAST_SYNC = "avro.last.sync";

  /*
   * number of records read since last sync occured
   */
  public static final String SHIFT_FROM_SYNC = "avro.shift.from.sync";

  public long lastSync = -1L;
  public int shiftFromSync = 0;

  private AvroReaderOffset(Map<String, Object> map) {
    fromMap(map);
  }

  public static AvroReaderOffset newInstance(Map<String, Object> map) {
    return new AvroReaderOffset(map);
  }

  /**
   * Updates internal status from reader
   * 
   * @param reader
   * @return true if next sync occured 
   * @throws IOException
   */
  public boolean updFromReader(DataFileReader reader) throws IOException {
    long prevSync = reader.previousSync();
    if (prevSync > 161 && this.lastSync != prevSync) {
      this.lastSync = prevSync;
      this.shiftFromSync = 0;
      return true;
    }
    return false;
  }

  /**
   * Updates reader from internal status
   * 
   * @param reader
   * @throws IOException
   */
  public void syncReader(DataFileReader reader) throws IOException {
    if (lastSync > 0) {
      reader.sync(lastSync - 16); // !!!!!!!!!!!!!!!!!!!! 16 sync size
    }
    if (shiftFromSync > 0) {
      for (int i = 0; i < shiftFromSync; i++) {
        reader.next();
      }

    }
  }

  public void incrementShift() {
    this.shiftFromSync++;
  }

  public Map<String, String> asMap() {
    return Collections.unmodifiableMap(new HashMap<String, String>() {
      {
        put(LAST_SYNC, "" + lastSync);
        put(SHIFT_FROM_SYNC, "" + shiftFromSync);
      }
    });
  }

  public void fromMap(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      this.lastSync = -1L;
      this.shiftFromSync = 0;
    } else {
      this.lastSync = Long.parseLong(map.getOrDefault(LAST_SYNC, "-1").toString());
      this.shiftFromSync = Integer.parseInt(map.getOrDefault(SHIFT_FROM_SYNC, "0").toString());
    }
  }
  
  @Override
  public String toString() {
    return asMap().toString();
  }

}
