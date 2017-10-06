package com.adaptris.kafka;

import static org.mockito.Matchers.anyString;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;

public class LoggingContextTest {

  private Logger log = LoggerFactory.getLogger(this.getClass());

  @Rule
  public TestName testName = new TestName();
  @Test
  public void testLogPartitions() {
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    List<PartitionInfo> partInfo = Arrays.asList(new PartitionInfo("topic", 1, null, new Node[0], new Node[0]));
    Mockito.when(kafkaConsumer.partitionsFor(anyString())).thenReturn(partInfo);
    LoggingContext.LOGGER.logPartitions(log, Arrays.asList("hello"), new LoggingContext() {
      @Override
      public boolean additionalDebug() {
        return false;
      }

    }, kafkaConsumer);
    LoggingContext.LOGGER.logPartitions(log, Arrays.asList("hello"), new LoggingContext() {
      @Override
      public boolean additionalDebug() {
        return true;
      }

    }, kafkaConsumer);
  }

}
