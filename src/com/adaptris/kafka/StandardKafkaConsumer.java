package com.adaptris.kafka;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageConsumerImp;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.util.TimeInterval;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around {@link KafkaConsumer}.
 * 
 * 
 * @author lchan
 * @config polling-apache-kafka-consumer
 * 
 */
@XStreamAlias("standard-apache-kafka-consumer")
@ComponentProfile(summary = "Receive messages via Apache Kafka", tag = "consumer,kafka", recommended = {NullConnection.class})
@DisplayOrder(order = {"destination", "additionalDebug"})
public class StandardKafkaConsumer extends AdaptrisMessageConsumerImp implements LoggingContext {

  private static final TimeInterval DEFAULT_RECV_TIMEOUT_INTERVAL = new TimeInterval(100L, TimeUnit.MILLISECONDS);

  @AdvancedConfig
  private Boolean additionalDebug;

  private transient KafkaConsumer<String, AdaptrisMessage> consumer;

  public StandardKafkaConsumer() {
  }

  public StandardKafkaConsumer(ConsumeDestination d) {
    this();
    setDestination(d);
  }

  @Override
  public void prepare() throws CoreException {

  }


  @Override
  public void init() throws CoreException {}

  @Override
  public void start() throws CoreException {
    try {
      Map<String, Object> props = retrieveConnection(KafkaConnection.class).buildConfig();
      props.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, getMessageFactory());
      consumer = createConsumer(props);
      List<String> topics = Arrays.asList(Args.notBlank(getDestination().getDestination(), "topics").split("\\s*,\\s*"));
      LoggingContext.LOGGER.logPartitions(log, topics, this, consumer);
      consumer.subscribe(topics);
      ManagedThreadFactory.createThread("KafkaConsumer", new MessageConsumerRunnable()).start();
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  public void stop() {
    closeConsumer();
  }

  @Override
  public void close() {
    closeConsumer();
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

  long receiveTimeoutMs() {
    return DEFAULT_RECV_TIMEOUT_INTERVAL.toMilliseconds();
  }

  /**
   */
  public Boolean getAdditionalDebug() {
    return additionalDebug;
  }

  /**
   *
   * @param b the logAllExceptions to set, default false
   */
  public void setAdditionalDebug(Boolean b) {
    additionalDebug = b;
  }

  @Override
  public boolean additionalDebug() {
    return getAdditionalDebug() != null ? getAdditionalDebug().booleanValue() : false;
  }

  KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
    return new KafkaConsumer<String, AdaptrisMessage>(config);
  }


  private class MessageConsumerRunnable implements Runnable {
    public void run() {
      do {
        try {
          ConsumerRecords<String, AdaptrisMessage> records = consumer.poll(receiveTimeoutMs());
          for (ConsumerRecord<String, AdaptrisMessage> record : records) {
            retrieveAdaptrisMessageListener().onAdaptrisMessage(record.value());
          }
        } catch (WakeupException e) {
          break;
        } catch (InvalidOffsetException | AuthorizationException e) {
          log.error(e.getMessage(), e);
        } catch (Exception e) {

        }
      } while (true);
    }
  }
}
