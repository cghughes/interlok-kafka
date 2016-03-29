package com.adaptris.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.NullConnection;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.kafka.KafkaProducer;

public class BasicKafkaProducerTest extends ProducerCase {

  private static Logger log = LoggerFactory.getLogger(BasicKafkaProducerTest.class);

  public BasicKafkaProducerTest(String name) {
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
    return ((StandaloneProducer) object).getProducer().getClass().getName();
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {

    KafkaProducer producer = new KafkaProducer();
    StandaloneProducer result = new StandaloneProducer(new NullConnection(), producer);

    return result;
  }

}
