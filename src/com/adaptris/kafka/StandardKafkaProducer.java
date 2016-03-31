package com.adaptris.kafka;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around {@link new org.apache.kafka.clients.producer.KafkaProducer}.
 * 
 * 
 * @author gdries
 * @author lchan
 * @config standard-apache-kafka-producer
 * 
 */
@XStreamAlias("standard-apache-kafka-producer")
@ComponentProfile(summary = "Deliver messages via Apache Kafka", tag = "producer,kafka", recommended = {NullConnection.class})
@DisplayOrder(order = {"recordKey", "producerConfig"})
public class StandardKafkaProducer extends ProduceOnlyProducerImp {

  @NotBlank
  private String recordKey;
  @NotNull
  @Valid
  private ProducerConfigBuilder producerConfig;

  private transient Producer<String, AdaptrisMessage> producer;

  public StandardKafkaProducer() {
    setProducerConfig(new BasicProducerConfigBuilder());
  }

  public StandardKafkaProducer(String recordKey, ProduceDestination d, ProducerConfigBuilder b) {
    setRecordKey(recordKey);
    setDestination(d);
    setProducerConfig(b);
  }



  @Override
  public void init() throws CoreException {
    producer = null;
  }

  @Override
  public void start() throws CoreException {
    try {
      producer = new KafkaProducer<>(getProducerConfig().build());
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw new CoreException(e);
    }
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  @Override
  public void close() {}

  @Override
  public void prepare() throws CoreException {}

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      String topic = destination.getDestination(msg);
      producer.send(new ProducerRecord<String, AdaptrisMessage>(topic, getRecordKey(), msg));
    } catch (CoreException e) {
      throw new ProduceException(e);
    }

  }

  public ProducerConfigBuilder getProducerConfig() {
    return producerConfig;
  }

  public void setProducerConfig(ProducerConfigBuilder pc) {
    this.producerConfig = Args.notNull(pc, "producer-config");
  }

  public String getRecordKey() {
    return recordKey;
  }

  /**
   * Set the key for the generated {@link ProducerRecord}.
   * 
   * @param k
   */
  public void setRecordKey(String k) {
    this.recordKey = Args.notNull(k, "key");
  }

}
