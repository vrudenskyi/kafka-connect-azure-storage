/**
 * 
 */
package com.mckesson.kafka.azure.storage.table;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mckesson.kafka.azure.storage.AzureStorageWriter;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableBatchOperation;

public class CloudTableStorageWriter extends AzureStorageWriter {
  private static final Logger log = LoggerFactory.getLogger(CloudTableStorageWriter.class);

  public static final int CHUNK_SIZE = 64000;
  public static final int AZURE_BATCH_SIZE = 95;

  private CloudTableClient tableClient;
  private String tablePattern;
  private String keyColumnName;
  private String valueColumnName;

  private LoadingCache<TopicPartition, CloudTable> tables;
  private Map<TopicPartition, Deque<TableBatchOperation>> batches = new HashMap<>();

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);

    try {
      CloudStorageAccount account = CloudStorageAccount.parse("");
      tableClient = account.createCloudTableClient();
    } catch (Exception e) {
      log.error("Failed to initilize table client", e);
      throw new ConnectException("Failed to initilize table client", e);
    }

    log.debug("create tables cache");
    tables = CacheBuilder.newBuilder().concurrencyLevel(4).weakKeys().maximumSize(1000).expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<TopicPartition, CloudTable>() {
          public CloudTable load(TopicPartition tp) throws Exception {
            String tableName = MessageFormat.format(tablePattern.replaceAll("\\{topic\\}", "{0}").replaceAll("\\{partition\\}", "{1}").replaceAll("\\{timestamp", "{2,date"),
                tp.topic(), tp.partition(), new Date());
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            cloudTable.createIfNotExists();
            return cloudTable;
          }

        });

    throw new NotImplementedException("CloudTableStorageWriter is not yet implemented");

  }

  protected CloudTable getTable(TopicPartition tp) throws ExecutionException {
    return tables.get(tp);
  }

  protected void addTable(TopicPartition tp) {
    tables.getUnchecked(tp);
  }

  protected void removeTable(TopicPartition tp) {
    tables.invalidate(tp);
  }

  public void write(Collection<SinkRecord> records) {

    for (SinkRecord sr : records) {
      TopicPartition tp = new TopicPartition(sr.topic(), sr.kafkaPartition());
      String partitionKey = tp.topic() + "_" + tp.partition();
      String rowKey = partitionKey + "_" + sr.kafkaOffset();
      DynamicTableEntity entity = new DynamicTableEntity(partitionKey, rowKey);
      entity.setTimestamp(new Date(sr.timestamp() == null ? System.currentTimeMillis() : sr.timestamp()));

      byte[] valueData = valueConverter.fromConnectData(sr.topic(), sr.valueSchema(), sr.value());
      byte[] keyData = keyConverter.fromConnectData(sr.topic(), sr.keySchema(), sr.key());

      if (valueData != null && valueData.length > 0) {
        // TODO: move azure chunk size to config
        if (valueData.length > CHUNK_SIZE) {
          // if size > 64K split on valueColumnName+idx
          entity.getProperties().put("chunks", new EntityProperty(valueData.length / CHUNK_SIZE));
          int chunkCounter = 0;
          for (int i = 0; i < valueData.length; i += CHUNK_SIZE) {
            entity.getProperties().put(valueColumnName + "_" + chunkCounter,
                new EntityProperty(new String(Arrays.copyOfRange(valueData, i, Math.min(valueData.length, i + CHUNK_SIZE)))));
            chunkCounter++;
          }
        } else {
          entity.getProperties().put(valueColumnName, new EntityProperty(new String(valueData)));
        }

      }

      if (keyData != null && keyData.length > 0) {
        entity.getProperties().put(keyColumnName, new EntityProperty(new String(keyData)));
      }
      getTableBatch(tp).insert(entity);
    }

  }

  /**
   * Note: the limitations on a batch operation are - up to 100 operations - all
   * operations must share the same PartitionKey - if a retrieve is used it can
   * be the only operation in the batch - the serialized batch payload must be 4
   * MB or less
   * 
   * @param tp
   * @return
   */
  private TableBatchOperation getTableBatch(TopicPartition tp) {
    if (!batches.containsKey(tp)) {
      batches.put(tp, new ArrayDeque<>());
    }
    Deque<TableBatchOperation> batchQueue = batches.get(tp);
    TableBatchOperation lastBatch = null;
    if (batchQueue.size() > 0) {
      lastBatch = batchQueue.getLast();
    }
    if (lastBatch == null || lastBatch.size() > AZURE_BATCH_SIZE) {
      lastBatch = new TableBatchOperation();
      batchQueue.addLast(lastBatch);
    }

    return lastBatch;

  }

  //@Override
  public void openPartitions(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      addTable(tp);
      getTableBatch(tp);
    }
  }

  //@Override
  public void closePartitions(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      try {
        flush(tp);
      } catch (Exception e) {
        log.error("Failed to flush partition:" + tp);
      }
      batches.remove(tp);
      removeTable(tp);

    }

  }

  //@Override
  public void close() {
    tables.cleanUp();

  }

  //@Override
  public void flush(TopicPartition tp) throws ConnectException {
    try {
      Deque<TableBatchOperation> queue = batches.get(tp);
      CloudTable table = tables.get(tp);
      if (table != null && queue != null) {

        while (!queue.isEmpty()) {
          TableBatchOperation tb = queue.pop();
          table.execute(tb);
        }
      }
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (StorageException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  //@Override
  public void flushAll() throws StorageException, IOException, ExecutionException {
    for (TopicPartition tp : batches.keySet()) {
      flush(tp);
    }

  }

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

}
