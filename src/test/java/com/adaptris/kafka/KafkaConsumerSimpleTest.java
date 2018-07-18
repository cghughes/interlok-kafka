package com.adaptris.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConsumerCase;
import com.adaptris.core.StandaloneConsumer;

public class KafkaConsumerSimpleTest extends ConsumerCase {

  private static Logger log = LoggerFactory.getLogger(KafkaConsumerSimpleTest.class);

  public KafkaConsumerSimpleTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneConsumer) object).getConsumer().getClass().getName() + "-SimpleConfigBuilder";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    StandardKafkaConsumer c = new StandardKafkaConsumer(new ConfiguredConsumeDestination("myTopic"));
    return new StandaloneConsumer(new KafkaConnection(new SimpleConfigBuilder("localhost:4242")), c);
  }

}
