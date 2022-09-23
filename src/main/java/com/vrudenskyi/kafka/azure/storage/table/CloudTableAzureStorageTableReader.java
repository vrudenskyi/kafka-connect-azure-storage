/**
 * 
 */
package com.vrudenskyi.kafka.azure.storage.table;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.core.Utility;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.EntityResolver;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.vrudenskyi.kafka.azure.storage.AzureStorageReader;
import com.vrudenskyi.kafka.azure.storage.AzureStorageTableOffset;
import com.vrudenskyi.kafka.azure.storage.ConfigValidators;
import com.vrudenskyi.kafka.connect.azure.storage.AzureStorageSourceConnector;
import com.microsoft.azure.storage.table.TableServiceEntity;

public class CloudTableAzureStorageTableReader extends AzureStorageReader {
  private static final Logger log = LoggerFactory.getLogger(CloudTableAzureStorageTableReader.class);

  /** Configs for table reader  */
  public static final String TABLE_DEFAULT_ENTITY_RESOLVER_CONFIG = "entityResolver";
  public static final String CONTAINER_NAME_CONFIG = "containerName";

  private static final ConfigDef TABLE_READER_CONFIG_DEF = new ConfigDef()
      .define(TABLE_DEFAULT_ENTITY_RESOLVER_CONFIG, ConfigDef.Type.CLASS, null, null, ConfigDef.Importance.HIGH, "EntityResolver implementation for table")
      .define(CONTAINER_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigValidators.CONTAINER_NAME_VALIDATOR, ConfigDef.Importance.HIGH, "Container name for blob");

  private String tableName;
  private CloudTable table;
  private Map<String, Object> offset;
  private Map<String, String> partition;
  private EntityResolver<Map<String, Object>> defaultEntityResolver;

  public CloudTableAzureStorageTableReader(String tableName, Map<String, String> partition, Map<String, Object> offset) {
    log.info("Initializing Table reader for table: {}", tableName);

    this.tableName = tableName;
    this.partition = partition;

    // create copy of offset or create new
    this.offset = new HashMap<>(offset == null ? 1 : offset.size());
    if (offset == null) {
      this.offset.put(AzureStorageTableOffset.TABLE_POSITION_KEY, "");
      log.debug("Initialized new offset: {}", this.offset);
    } else {
      this.offset.putAll(offset);
      log.debug("Current offset: {}", this.offset);
    }

    try {
      CloudTableClient tableClient = storageAccount.createCloudTableClient();
      table = tableClient.getTableReference(this.tableName);
      if (!table.exists()) {
        throw new ConnectException("Table does not exist: " + this.tableName);
      }
    } catch (Exception e) {
      log.error("Failed to initilize cloud table", e);
      if (e instanceof ConnectException)
        throw (ConnectException) e;
      throw new ConnectException("Failed to initilize cloud table", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    final SimpleConfig conf = new SimpleConfig(TABLE_READER_CONFIG_DEF, sourceConfig.originalsWithPrefix(AzureStorageSourceConnector.AZURE_SOURCE_READER_CONFIG + "."));
    defaultEntityResolver = conf.getConfiguredInstance(TABLE_DEFAULT_ENTITY_RESOLVER_CONFIG, EntityResolver.class);

  }

  @Override
  public void close() {
    table = null;

  }

  @Override
  public boolean read(int maxRecords, List<SourceRecord> recs, OffsetStorageReader offsetStorageReader) {
    // rowkey is a timestamp
    String rowKey = offset.getOrDefault(AzureStorageTableOffset.TABLE_POSITION_KEY, Utility.getJavaISO8601Time(new Date(0L))).toString();

    log.debug("Read from CloudTable: maxRecords: {}, offset:{},  rowkey:{}", maxRecords, offset, rowKey);

    if (StringUtils.isBlank(rowKey)) {
      rowKey = Utility.getJavaISO8601Time(new Date(0L));
    }

    // Create the range scan query based on timestamp
    String tsFilter = TableQuery.generateFilterCondition(AzureStorageTableOffset.TABLE_TIMESTAMP_COLUMN, QueryComparisons.GREATER_THAN_OR_EQUAL, rowKey, EdmType.DATE_TIME);
    TableQuery<?> rangeQuery = TableQuery.from(TableServiceEntity.class).where(tsFilter);
    //rangeQuery.setTakeCount(maxRecords + 1);

    Iterable<Map<String, Object>> queryResult = table.execute(rangeQuery, defaultEntityResolver);

    boolean hasMoreData = false;
    log.debug("Query successfully executed. Fetching data...");
    for (Map<String, Object> r : queryResult) {
      SourceRecord rec = extractRecord(r);
      recs.add(rec);
      if (recs.size() > maxRecords) {
        hasMoreData = true;
        break;
      }
    }

    if (hasMoreData) {
      int removedCounter = 0;
      Object lastOffset = recs.get(recs.size() - 1).sourceOffset().get(AzureStorageTableOffset.TABLE_POSITION_KEY);
      log.debug("Removing tail records with offset: {}", lastOffset);
      // Iterate in reverse.
      ListIterator<SourceRecord> li = recs.listIterator(recs.size());
      while (li.hasPrevious()) {
        SourceRecord r = li.previous();
        Object rOffset = r.sourceOffset().get(AzureStorageTableOffset.TABLE_POSITION_KEY);
        if (lastOffset.equals(rOffset)) {
          li.remove();
          removedCounter++;
        } else {
          // offset.put(AzureStorageTableOffset.TABLE_POSITION_KEY, rOffset);
          log.debug("Removed {} records, with offset: {}, last result offset: {}", removedCounter, lastOffset, rOffset);
          break;
        }

      }
    }

    return hasMoreData;
  }

  private SourceRecord extractRecord(Map<String, Object> r) {
    // update offset
    Date ts = (Date) r.get(DefaultStorageTableEntityResolver.TIMESTAMP_KEY);
    this.offset.put(AzureStorageTableOffset.TABLE_POSITION_KEY, Utility.getJavaISO8601Time(ts));

    try {
      return new SourceRecord(this.partition, new HashMap<>(this.offset), (String) r.get(DefaultStorageTableEntityResolver.ROW_KEY), null,
          new ObjectMapper().writeValueAsString(r));
    } catch (JsonProcessingException e) {
      log.error("Failed to extract data from record: {}, r");
      return new SourceRecord(this.partition, this.offset, (String) r.get(DefaultStorageTableEntityResolver.ROW_KEY), null, r.toString());
    }

  }

}
