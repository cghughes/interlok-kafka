package com.adaptris.kafka;

import java.util.Properties;

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
    none,
    /**
     * Equivalent to {@code 1} when specifying the number of acks.
     * 
     */
    local,
    /**
     * Equivalent to {@code all}.
     * 
     */
    all
  };


  Properties build();
}
