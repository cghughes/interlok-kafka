package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.adaptris.core.CoreException;

public interface ConfigBuilder {
  String DEFAULT_KEY_SERIALIZER = StringSerializer.class.getName();
  String DEFAULT_VALUE_SERIALIZER = AdaptrisMessageSerializer.class.getName();
  String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
  String DEFAULT_VALUE_DESERIALIZER = AdaptrisMessageDeserializer.class.getName();

  String KEY_DESERIALIZER_FACTORY_CONFIG = "adaptris.message.factory";

  enum CompressionType {
    /**
     * Equivalent to {@code none} when specifying the compression type
     * 
     */
    none,
    /**
     * Equivalent to {@code gzip} when specifying the compression type
     * 
     */
    gzip,
    /**
     * Equivalent to {@code snappy} when specifying the compression type
     * 
     */
    snappy,
    /**
     * Equivalent to {@code lz4} when specifying the compression type
     * 
     */
    lz4;
    
    static String toConfigValue(CompressionType c) {
      if (c != null) {
        return c.name();
      }
      return null;
    }
  };

  enum Acks {
    /**
     * Equivalent to {@code 0} when specifying the number of acks.
     * 
     */
    none("0"),
    /**
     * Equivalent to {@code 1} when specifying the number of acks.
     * 
     */
    local("1"),
    /**
     * Equivalent to {@code all}.
     * 
     */
    all("all");

    private String actual;

    Acks(String s) {
      actual = s;
    }

    String actualValue() {
      return actual;
    }

    static String toConfigValue(Acks c) {
      if (c != null) {
        return c.actualValue();
      }
      return null;
    }
  };

  Map<String, Object> build() throws CoreException;

}
