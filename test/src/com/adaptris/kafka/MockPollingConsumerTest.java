package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.BaseCase;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.FixedIntervalPoller;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;

public class MockPollingConsumerTest {

  private static Logger log = LoggerFactory.getLogger(MockPollingConsumerTest.class);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Test
  public void testLifecycle() throws Exception {
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    PollingKafkaConsumer consumer =
        new PollingKafkaConsumer(new ConfiguredConsumeDestination(text), new BasicConsumerConfigBuilder()) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            return kafkaConsumer;
          }
        };
    Mockito.when(records.count()).thenReturn(0);
    Mockito.when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(consumer);
    try {
      sc.prepare();
      LifecycleHelper.init(sc);
      LifecycleHelper.start(sc);
      LifecycleHelper.stop(sc);
      LifecycleHelper.close(sc);
    } finally {
      BaseCase.stop(sc);
    }
  }

  @Test
  public void testLifecycle_WithException() throws Exception {
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    PollingKafkaConsumer consumer =
        new PollingKafkaConsumer(new ConfiguredConsumeDestination(text), new BasicConsumerConfigBuilder()) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            throw new RuntimeException(text);
          }
        };
    Mockito.when(records.count()).thenReturn(0);
    Mockito.when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(consumer);
    try {
      sc.prepare();
      LifecycleHelper.init(sc);
      LifecycleHelper.start(sc);
      fail();
    } catch (CoreException e) {
      assertNotNull(e.getCause());
      assertEquals(text, e.getCause().getMessage());
      BaseCase.stop(sc);
    }
  }


  @Test
  public void testConsume() throws Exception {
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);

    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    ConsumerRecord<String, AdaptrisMessage> record = new ConsumerRecord<String, AdaptrisMessage>(text, 0, 0, text, msg);

    PollingKafkaConsumer consumer =
        new PollingKafkaConsumer(new ConfiguredConsumeDestination(text), new BasicConsumerConfigBuilder()) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            return kafkaConsumer;
          }
        };
        
    consumer.setPoller(new FixedIntervalPoller(new TimeInterval(100L, TimeUnit.MILLISECONDS)));

    Mockito.when(records.count()).thenReturn(1);
    Mockito.when(records.iterator())
        .thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>(Arrays.asList(record)).iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(consumer);
    MockMessageListener mock = new MockMessageListener();
    sc.registerAdaptrisMessageListener(mock);
    try {
      sc.prepare();
      BaseCase.start(sc);
      BaseCase.waitForMessages(mock, 1);
      assertTrue(mock.getMessages().size() >= 1);
      AdaptrisMessage consumed = mock.getMessages().get(0);
      assertEquals(text, consumed.getContent());
    } finally {
      LifecycleHelper.stop(sc);
      LifecycleHelper.close(sc);
    }
  }


}
