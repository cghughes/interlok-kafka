package com.adaptris.kafka;

import static org.junit.Assert.fail;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.kafka.ProducerConfigBuilder.Acks;
import com.adaptris.kafka.embedded.KafkaServerWrapper;
import com.adaptris.util.KeyValuePair;

public class StandardKafkaProducerTest {

  private static Logger log = LoggerFactory.getLogger(StandardKafkaProducerTest.class);

  private static transient KafkaServerWrapper wrapper;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    wrapper = new KafkaServerWrapper(2);
    wrapper.start();
  }

  @AfterClass
  public static void teardownClass() {
    wrapper.shutdown();
  }

  @Test
  public void testStart_BadConfig() throws Exception {
    String text = testName.getMethodName();
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    // No BootstrapServer, so we're duff.
    StandaloneProducer p = new StandaloneProducer(createProducer(text, new ConfiguredProduceDestination(text), builder));
    try {
      LifecycleHelper.init(p);
      try {
        LifecycleHelper.start(p);
        fail();
      } catch (CoreException expected) {

      }
    } finally {
      LifecycleHelper.stop(p);
      LifecycleHelper.close(p);
    }
  }

  @Test
  public void testLifecycle() throws Exception {
    String text = testName.getMethodName();
    StandardKafkaProducer p = createProducer(wrapper.getConnections(), text, text);
    try {
      LifecycleHelper.init(p);
      LifecycleHelper.start(p);
      LifecycleHelper.stop(p);
      LifecycleHelper.close(p);
    } finally {
      LifecycleHelper.stop(p);
      LifecycleHelper.close(p);
    }
  }


  @Test
  public void testSend() throws Exception {
    try {
      String text = testName.getMethodName();
      StandaloneProducer p = new StandaloneProducer(createProducer(wrapper.getConnections(), text, text));
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
      ServiceCase.execute(p, msg);
    } finally {
    }
  }


  private StandardKafkaProducer createProducer(String bootstrapServer, String recordKey, String topic) {
    AdvancedProducerConfigBuilder builder = new AdvancedProducerConfigBuilder();
    builder.getConfig().add(new KeyValuePair(ProducerConfig.ACKS_CONFIG, Acks.none.actualValue()));
    builder.getConfig().add(new KeyValuePair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    // Change MAX_BLOCK to stop each test from taking ~60000 which is the default block...
    // Can't figure out why KafkaProducer is why it is atm.
    builder.getConfig().add(new KeyValuePair(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100"));
    return createProducer(recordKey, new ConfiguredProduceDestination(topic), builder);
  }

  private StandardKafkaProducer createProducer(String recordKey, ProduceDestination d, ProducerConfigBuilder builder) {
    return new StandardKafkaProducer(recordKey, d, builder);

  }
}
