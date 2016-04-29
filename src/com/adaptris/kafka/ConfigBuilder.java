package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.adaptris.core.CoreException;

public interface ConfigBuilder {
  String DEFAULT_KEY_SERIALIZER = StringSerializer.class.getName();
  String DEFAULT_VALUE_SERIALIZER = AdaptrisMessageSerializer.class.getName();
  String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
  String DEFAULT_VALUE_DESERIALIZER = AdaptrisMessageDeserializer.class.getName();

  String KEY_DESERIALIZER_FACTORY_CONFIG = "adaptris.message.factory";

  Map<String, Object> build() throws CoreException;
}
