/**
 * 
 */
package com.mckesson.kafka.azure.storage.table;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.EntityResolver;

public class DefaultStorageTableEntityResolver implements EntityResolver<Map<String, Object>> {
  
  public final static String TIMESTAMP_KEY = "timestamp";  
  public final static String ROW_KEY = "rowKey";
  public final static String PARTITION_KEY = "partitionKey";
  public final static String ETAG_KEY = "etagKey";
  

  @Override
  public Map<String, Object> resolve(String partitionKey, String rowKey, Date timeStamp, HashMap<String, EntityProperty> properties, String etag) throws StorageException {
    Map<String, Object> result = new HashMap<>();
    result.put(PARTITION_KEY, partitionKey);
    result.put(ROW_KEY, rowKey);
    result.put(TIMESTAMP_KEY, timeStamp);
    result.put(ETAG_KEY, etag);
    for (Entry<String, EntityProperty> e : properties.entrySet()) {
      if (e.getValue() != null) {
        result.put(e.getKey(), e.getValue().getValueAsString());
      }
    }
    return result;
  }

}
