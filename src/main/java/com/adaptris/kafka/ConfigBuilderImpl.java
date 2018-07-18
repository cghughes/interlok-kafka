package com.adaptris.kafka;

import java.util.Map;

public abstract class ConfigBuilderImpl implements ConfigBuilder {

  protected static Map<String, Object> addEntry(Map<String, Object> properties, String propertyName, Object o) {
    if (o != null) {
      properties.put(propertyName, o);
    }
    return properties;
  }
}
