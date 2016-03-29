package com.adaptris.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("apache-kafka-producer")
public class KafkaProducer extends ProduceOnlyProducerImp {

  private String bootstrapServers = "localhost:4242";
  
  private int retries;
  
  private int batchSize = 16384;
  
  private int lingerMs = 1;
  
  private int bufferMemory = 33554432;
  
  private String key;
  
  private transient Producer<String, String> producer;
  
  @Override
  public void init() throws CoreException {
    // TODO Auto-generated method stub
  }

  @Override
  public void start() throws CoreException {
    Properties props = new Properties();
    props.put("bootstrap.servers", getBootstrapServers());
    props.put("acks", "all");
    props.put("retries", getRetries());
    props.put("batch.size", getBatchSize());
    props.put("linger.ms", getLingerMs());
    props.put("buffer.memory", getBufferMemory());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }

  @Override
  public void stop() {
    producer.close();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public void prepare() throws CoreException {
    // TODO Auto-generated method stub
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      String topic = destination.getDestination(msg);
      producer.send(new ProducerRecord<String, String>(topic, getKey(), msg.getContent()));
    } catch (CoreException e) {
      throw new ProduceException(e);
    }
    
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public int getRetries() {
    return retries;
  }

  public void setRetries(int retries) {
    this.retries = retries;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getLingerMs() {
    return lingerMs;
  }

  public void setLingerMs(int lingerMs) {
    this.lingerMs = lingerMs;
  }

  public int getBufferMemory() {
    return bufferMemory;
  }

  public void setBufferMemory(int bufferMemory) {
    this.bufferMemory = bufferMemory;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }
  
}
