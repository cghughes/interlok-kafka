package com.adaptris.kafka;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConfigBuilderImpl implements ConfigBuilder {

  protected transient Logger log = LoggerFactory.getLogger(this.getClass());

  protected static Map<String, Object> addEntry(Map<String, Object> properties, String propertyName, Object o) {
    if (o != null) {
      properties.put(propertyName, o);
    }
    return properties;
  }

}
