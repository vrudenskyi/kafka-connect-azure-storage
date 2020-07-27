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

import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.microsoft.azure.storage.CloudStorageAccount;

/**
 *  
 */
public class AzureStorage implements Configurable {

  public static final String CONNECTION_STRING_CONFIG = "azure.sa.connection.string";
  public static final String SA_NAME_CONFIG = "azure.sa.name";

  public static final String AZURE_STORAGE_KEY_CONVERTER_CONFIG = "azure.storage.key.converter";
  public static final String AZURE_STORAGE_VALUE_CONVERTER_CONFIG = "azure.storage.value.converter";
  public static final String AZURE_STORAGE_HEADERS_CONVERTER_CONFIG = "azure.storage.headers.converter";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CONNECTION_STRING_CONFIG, Type.STRING, null, Importance.HIGH, "Storage account connection  string")
      .define(SA_NAME_CONFIG, Type.STRING, null, Importance.HIGH, "Storage account  name")
      .define(AZURE_STORAGE_KEY_CONVERTER_CONFIG, Type.CLASS, StringConverter.class, Importance.MEDIUM, "Record key Converter. Default: STRING")
      .define(AZURE_STORAGE_VALUE_CONVERTER_CONFIG, Type.CLASS, JsonConverter.class, Importance.MEDIUM, "Record value Converter. Default: JSON")
      .define(AZURE_STORAGE_HEADERS_CONVERTER_CONFIG, Type.CLASS, JsonConverter.class, Importance.MEDIUM, "Record headers Converter. Default: JSON");


  
  
  
  protected CloudStorageAccount storageAccount;
  protected Converter valueConverter;
  protected Converter keyConverter;
  protected HeaderConverter headersConverter;

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig storageConfig = new SimpleConfig(CONFIG_DEF, configs);
    //init storage account
    String storageConnectionString = storageConfig.getString(CONNECTION_STRING_CONFIG);
    try {
      storageAccount = CloudStorageAccount.parse(storageConnectionString);
    } catch (Exception e) {
      throw new ConnectException("Failed to create storage account", e);
    }

    Class<?> convClass = storageConfig.getClass(AZURE_STORAGE_KEY_CONVERTER_CONFIG);
    if (!Converter.class.isAssignableFrom(convClass))
      throw new KafkaException(convClass.getName() + " is not an instance of Converter.class");
    Object convObj = Utils.newInstance(convClass);
    if (convObj instanceof Configurable) {
      Map<String, Object> convOverrides = storageConfig.originalsWithPrefix(AZURE_STORAGE_KEY_CONVERTER_CONFIG);
      convOverrides.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.name().toLowerCase());
      ((Configurable) convObj).configure(convOverrides);
      keyConverter = (Converter) convObj;
    }

    convClass = storageConfig.getClass(AZURE_STORAGE_VALUE_CONVERTER_CONFIG);
    if (!Converter.class.isAssignableFrom(convClass))
      throw new KafkaException(convClass.getName() + " is not an instance of Converter.class");
    convObj = Utils.newInstance(convClass);
    if (convObj instanceof Configurable) {
      Map<String, Object> convOverrides = storageConfig.originalsWithPrefix(AZURE_STORAGE_VALUE_CONVERTER_CONFIG);
      convOverrides.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.name().toLowerCase());
      ((Configurable) convObj).configure(convOverrides);
      valueConverter = (Converter) convObj;
    }

    convClass = storageConfig.getClass(AZURE_STORAGE_HEADERS_CONVERTER_CONFIG);
    if (!Converter.class.isAssignableFrom(convClass))
      throw new KafkaException(convClass.getName() + " is not an instance of Converter.class");
    convObj = Utils.newInstance(convClass);
    if (convObj instanceof Configurable) {
      Map<String, Object> convOverrides = storageConfig.originalsWithPrefix(AZURE_STORAGE_HEADERS_CONVERTER_CONFIG);
      convOverrides.put(ConverterConfig.TYPE_CONFIG, ConverterType.HEADER.name().toLowerCase());
      ((Configurable) convObj).configure(convOverrides);
      headersConverter = (HeaderConverter) convObj;
    }

  }

}
