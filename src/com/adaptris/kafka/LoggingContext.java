package com.adaptris.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;

import com.adaptris.core.AdaptrisMessage;

public interface LoggingContext {
  public static final PartitionLogger LOGGER = new PartitionLogger();

  boolean additionalDebug();
  
  Logger logger();

}

class PartitionLogger {

  void logPartitions(LoggingContext ctx, List<String> topics, 
                     KafkaConsumer<String, AdaptrisMessage> c) {
    if (ctx.additionalDebug()) {
      for (String t : topics) {
        for (PartitionInfo partition : c.partitionsFor(t)) {
          ctx.logger().trace("Partition Info [{}]", partition);
        }
      }
    }
  }
}
