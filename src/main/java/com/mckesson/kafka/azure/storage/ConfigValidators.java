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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class ConfigValidators {
  
  public static final ConfigDef.Validator CONTAINER_NAME_VALIDATOR = new ConfigDef.Validator() {
    @Override
    public void ensureValid(String name, Object value) {
      String storageContainer = value == null ? null : value.toString();
      if (storageContainer == null || storageContainer.length() < 3 || storageContainer.length() > 63 || !storageContainer.matches("^[a-z0-9]+(-[a-z0-9]+)*$")) {
        throw new ConfigException("storage container name does not match the rules. see https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/");
      }
    }
  };

}
