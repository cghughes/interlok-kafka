package com.adaptris.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.NullConnection;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.kafka.ProducerConfigBuilder.Acks;
import com.adaptris.kafka.ProducerConfigBuilder.CompressionType;

public class BasicKafkaProducerExampleTest extends ProducerCase {

  private static Logger log = LoggerFactory.getLogger(BasicKafkaProducerExampleTest.class);

  public BasicKafkaProducerExampleTest(String name) {
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
    return ((StandaloneProducer) object).getProducer().getClass().getName() + "-BasicProducerConfig";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {

    BasicProducerConfigBuilder b = new BasicProducerConfigBuilder("localhost:4242");
    b.setCompressionType(CompressionType.none);
    b.setAcks(Acks.all);
    StandardKafkaProducer producer =
        new StandardKafkaProducer("MyProducerRecordKey", new ConfiguredProduceDestination("MyTopic"), b);
    StandaloneProducer result = new StandaloneProducer(new NullConnection(), producer);

    return result;
  }

  private StandardKafkaProducer createProducer(String bootstrapServer, String topic, String recordKey) {
    BasicProducerConfigBuilder b = new BasicProducerConfigBuilder(bootstrapServer);
    StandardKafkaProducer producer = new StandardKafkaProducer(recordKey, new ConfiguredProduceDestination(topic), b);
    return producer;
  }

}
