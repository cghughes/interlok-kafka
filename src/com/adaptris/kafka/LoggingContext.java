package com.adaptris.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;

import com.adaptris.core.AdaptrisMessage;

public interface LoggingContext {
  public static final PartitionLogger LOGGER = new PartitionLogger();

  
  boolean additionalDebug();
  
}

class PartitionLogger {

  void logPartitions(Logger log, List<String> topics, LoggingContext ctx,
                     KafkaConsumer<String, AdaptrisMessage> c) {
    if (ctx.additionalDebug()) {
      for (String t : topics) {
        for (PartitionInfo partition : c.partitionsFor(t)) {
          log.trace("Partition Info [{}]", partition);
        }
      }
    }
  }
}
