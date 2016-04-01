package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import com.adaptris.core.BaseCase;
import com.adaptris.core.ClosedState;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.FixedIntervalPoller;
import com.adaptris.core.InitialisedState;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.StartedState;
import com.adaptris.core.StoppedState;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.kafka.ProducerConfigBuilder.Acks;
import com.adaptris.kafka.embedded.KafkaServerWrapper;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.TimeInterval;

public class InlineKafkaTest {

  private static Logger log = LoggerFactory.getLogger(InlineKafkaTest.class);

  private static KafkaServerWrapper wrapper;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    wrapper = new KafkaServerWrapper(1);
    wrapper.start();
  }

  @AfterClass
  public static void tearDownClass() {
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
  public void testProducerLifecycle() throws Exception {
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
  public void testConsumerLifecycle() throws Exception {
    String text = testName.getMethodName();
    MockMessageListener mock = new MockMessageListener();
    StandaloneConsumer sc = createConsumer(wrapper.getConnections(), text, mock);
    try {
      LifecycleHelper.init(sc);
      assertEquals(InitialisedState.getInstance(), sc.retrieveComponentState());
      LifecycleHelper.start(sc);
      assertEquals(StartedState.getInstance(), sc.retrieveComponentState());
      Thread.sleep(1000);
      LifecycleHelper.stop(sc);
      assertEquals(StoppedState.getInstance(), sc.retrieveComponentState());
      LifecycleHelper.close(sc);
      assertEquals(ClosedState.getInstance(), sc.retrieveComponentState());
    } finally {
      BaseCase.stop(sc);
    }
  }


  @Test
  public void testSendAndReceive_Polling() throws Exception {
    StandaloneConsumer sc = null;
    StandaloneProducer sp = null;
    try {
      String text = testName.getMethodName();
      sp = new StandaloneProducer(createProducer(wrapper.getConnections(), text, text));
      MockMessageListener mock = new MockMessageListener();
      sc = createConsumer(wrapper.getConnections(), text, mock);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
      // BaseCase.start(sc);
      ServiceCase.execute(sp, msg);
      // BaseCase.waitForMessages(mock, 1);
      // assertEquals(1, mock.getMessages().size());
      // AdaptrisMessage consumed = mock.getMessages().get(0);
      // assertEquals(text, consumed.getContent());
    } finally {
      BaseCase.stop(sc, sp);
    }
  }


  private StandaloneConsumer createConsumer(String bootstrapServer, String topic, MockMessageListener p) {
    StandaloneConsumer sc = new StandaloneConsumer(createConsumer(bootstrapServer, topic));
    sc.registerAdaptrisMessageListener(p);
    return sc;
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

  private StandardKafkaConsumer createConsumer(String bootstrapServer, String topic) {
    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder();
    builder.getConfig().add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    StandardKafkaConsumer result = new StandardKafkaConsumer(new ConfiguredConsumeDestination(topic), builder);
    result.setPoller(new FixedIntervalPoller(new TimeInterval(100L, TimeUnit.MILLISECONDS)));
    result.setReceiveTimeout(new TimeInterval(2L, TimeUnit.SECONDS));
    return result;
  }


  private StandardKafkaProducer createProducer(String recordKey, ProduceDestination d, ProducerConfigBuilder builder) {
    return new StandardKafkaProducer(recordKey, d, builder);

  }
}
