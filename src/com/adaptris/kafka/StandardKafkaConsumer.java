package com.adaptris.kafka;

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.util.Args;
import com.adaptris.util.GuidGenerator;
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
  private Boolean additionalDebug;

  private transient KafkaConsumer<String, AdaptrisMessage> consumer;
  private transient static GuidGenerator GUID = new GuidGenerator();

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
    try {
      Map<String, Object> props = getConsumerConfig().build();
      if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultIfEmpty(getUniqueId(), GUID.safeUUID()));
      }
      props.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, getMessageFactory());
      consumer = new KafkaConsumer<>(props);
      List<String> topics = asList(getDestination().getDestination());
      logPartitions(topics);
      consumer.subscribe(topics);
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw new CoreException(e);
    }
    super.start();
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
      if (consumer == null)
        return 0;
      log.trace("Going to Poll with timeout {}", receiveTimeoutMs());
      ConsumerRecords<String, AdaptrisMessage> records = consumer.poll(receiveTimeoutMs());
      for (ConsumerRecord<String, AdaptrisMessage> record : records) {
        retrieveAdaptrisMessageListener().onAdaptrisMessage(record.value());
        proc++;
      }
    } catch (Exception e) {
      log.warn("Exception during poll(), waiting for next scheduled poll");
      if (additionalDebug()) {
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
  public Boolean getAdditionalDebug() {
    return additionalDebug;
  }

  /**
   * Whether or not to log all stacktraces.
   *
   * @param b the logAllExceptions to set, default false
   */
  public void setAdditionalDebug(Boolean b) {
    additionalDebug = b;
  }

  boolean additionalDebug() {
    return getAdditionalDebug() != null ? getAdditionalDebug().booleanValue() : false;
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

  private void logPartitions(List<String> topics) {
    if (additionalDebug()) {
      for (String topic : topics) {
        for (PartitionInfo partition : consumer.partitionsFor(topic)) {
          log.trace("Partition Info [{}]", partition);
        }
      }
    }

  }
}
