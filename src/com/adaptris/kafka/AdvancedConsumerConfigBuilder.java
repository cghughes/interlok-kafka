package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.adaptris.core.CoreException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ConsumerConfigBuilder} that exposes all configuration.
 * 
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various
 * SSL passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be decoded using
 * {@link Password#decode(String)} appropriately.
 * </p>
 * <p>
 * Regardless of what is configured; the {@code key.deserializer} property is
 * fixed to be a {@link StringDeserializer}; and the {@code value.deserializer} property is always an
 * {@link AdaptrisMessageDeserializer}.
 * </p>
 * 
 * @author lchan
 * @config kafka-advanced-consumer-config
 */
@XStreamAlias("kafka-advanced-consumer-config")
public class AdvancedConsumerConfigBuilder extends AdvancedConfigBuilderImpl implements ConsumerConfigBuilder {

  public AdvancedConsumerConfigBuilder() {
    super();
  }


  public AdvancedConsumerConfigBuilder(KeyValuePairSet cfg) {
    super(cfg);
  }

  @Override
  public Map<String, Object> build() throws CoreException {
    Map<String, Object> result = convertAndDecode(getConfig());
    result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
    return result;
  }

}
