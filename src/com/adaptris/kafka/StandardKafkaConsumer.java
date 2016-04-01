package com.adaptris.kafka;

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.util.Args;
import com.adaptris.util.TimeInterval;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around {@link KafkaConsumer}.
 * 
 * 
 * @author lchan
 * @config standard-apache-kafka-consumer
 * 
 */
@XStreamAlias("standard-apache-kafka-consumer")
@ComponentProfile(summary = "Receive messages via Apache Kafka", tag = "consumer,kafka", recommended = {NullConnection.class})
@DisplayOrder(order = {"destination", "consumerConfig", "receiveTimeout", "logAllExceptions"})
public class StandardKafkaConsumer extends AdaptrisPollingConsumer {

  private static final TimeInterval DEFAULT_RECV_TIMEOUT_INTERVAL = new TimeInterval(2L, TimeUnit.SECONDS);

  @NotNull
  @Valid
  private ConsumerConfigBuilder consumerConfig;
  @AdvancedConfig
  private TimeInterval receiveTimeout;
  @AdvancedConfig
  private Boolean logAllExceptions;

  private transient KafkaConsumer<String, AdaptrisMessage> consumer;

  public StandardKafkaConsumer() {
    setConsumerConfig(new BasicConsumerConfigBuilder());
  }

  public StandardKafkaConsumer(ConsumeDestination d, ConsumerConfigBuilder b) {
    setConsumerConfig(b);
    setDestination(d);
  }



  @Override
  public void init() throws CoreException {
    super.init();
  }

  @Override
  public void start() throws CoreException {
    super.start();
    try {
      Map<String, Object> props = getConsumerConfig().build();
      props.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, getMessageFactory());
      consumer = new KafkaConsumer<>(props);
      consumer.subscribe(asList(getDestination().getDestination()));
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw new CoreException(e);
    }
  }

  @Override
  public void stop() {
    closeConsumer();
    super.stop();
  }

  @Override
  public void close() {
    super.close();
  }


  @Override
  protected void prepareConsumer() throws CoreException {

  }

  private void closeConsumer() {
    try {
      if (consumer != null) {
        consumer.wakeup();
        consumer.close();
        consumer = null;
      }
    } catch (RuntimeException e) {

    }

  }

  @Override
  protected int processMessages() {
    int proc = 0;
    try {
      System.out.println("Going to Poll");
      ConsumerRecords<String, AdaptrisMessage> records = consumer.poll(receiveTimeoutMs());
      System.out.println("Got Records : " + records.count());
      for (ConsumerRecord<String, AdaptrisMessage> record : records) {
        retrieveAdaptrisMessageListener().onAdaptrisMessage(record.value());
        proc++;
      }
    } catch (Exception e) {
      log.warn("Exception during poll(), waiting for next scheduled poll");
      if (logAllExceptions()) {
        log.warn(e.getMessage(), e);
      }
    }
    return proc;
  }

  public ConsumerConfigBuilder getConsumerConfig() {
    return consumerConfig;
  }

  public void setConsumerConfig(ConsumerConfigBuilder pc) {
    this.consumerConfig = Args.notNull(pc, "consumer-config");
  }

  long receiveTimeoutMs() {
    return getReceiveTimeout() != null ? getReceiveTimeout().toMilliseconds() : DEFAULT_RECV_TIMEOUT_INTERVAL.toMilliseconds();
  }

  public TimeInterval getReceiveTimeout() {
    return receiveTimeout;
  }


  /**
   * @return the logAllExceptions
   */
  public Boolean getLogAllExceptions() {
    return logAllExceptions;
  }

  /**
   * Whether or not to log all stacktraces.
   *
   * @param b the logAllExceptions to set, default true
   */
  public void setLogAllExceptions(Boolean b) {
    logAllExceptions = b;
  }

  boolean logAllExceptions() {
    return getLogAllExceptions() != null ? getLogAllExceptions().booleanValue() : true;
  }

  /**
   * Set the receive timeout.
   * 
   * @param rt the receive timout.
   */
  public void setReceiveTimeout(TimeInterval rt) {
    this.receiveTimeout = rt;
  }

  private static List<String> asList(String s) {
    if (isEmpty(s)) {
      return Collections.emptyList();
    }
    return Arrays.asList(s.split("\\s*,\\s*", -1));
  }
}
