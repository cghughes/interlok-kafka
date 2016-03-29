package com.adaptris.kafka;

import java.util.Properties;

import com.adaptris.core.CoreException;

public interface ProducerConfigBuilder {


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
    lz4
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
  };


  Properties build() throws CoreException;
}
