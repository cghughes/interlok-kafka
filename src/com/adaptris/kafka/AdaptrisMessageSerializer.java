package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.adaptris.core.AdaptrisMessage;

/**
 * Serializer implementation for {@link AdaptrisMessage} objects.
 * 
 * @author lchan
 *
 */
public class AdaptrisMessageSerializer implements Serializer<AdaptrisMessage> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, AdaptrisMessage data) {
    return data.getPayload();
  }

  @Override
  public void close() {}

}
