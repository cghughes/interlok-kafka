package com.adaptris.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ConfigBuilder} that exposes all configuration.
 * 
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various SSL
 * passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be decoded using {@link Password#decode(String)}
 * appropriately.
 * </p>
 * <p>
 * Regardless of what is configured; the {@code key.deserializer} property is fixed to be a {@link StringDeserializer}; and the
 * {@code value.deserializer} property is always an {@link AdaptrisMessageDeserializer}.
 * </p>
 * 
 * @config kafka-advanced-config-builder
 */
@XStreamAlias("kafka-advanced-config-builder")
public class AdvancedConfigBuilder implements ConfigBuilder {
  /**
   * List of keys that can be {@code encoded} via {@link Password}. Currently set to :
   * {@code ssl.key.password, ssl.keystore.password, ssl.truststore.password}
   */
  public static final List<String> PASSWORD_KEYS = Arrays.asList(new String[]
  {
      SslConfigs.SSL_KEY_PASSWORD_CONFIG, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  });


  @NotNull
  @Valid
  @AutoPopulated
  private KeyValuePairSet config;


  public AdvancedConfigBuilder() {
    setConfig(new KeyValuePairSet());
  }

  public AdvancedConfigBuilder(KeyValuePairSet cfg) {
    setConfig(cfg);
  }

  /**
   * @return the config
   */
  public KeyValuePairSet getConfig() {
    return config;
  }

  /**
   * @param config the config to set
   */
  public void setConfig(KeyValuePairSet config) {
    this.config = Args.notNull(config, "config");
  }

  @Override
  public Map<String, Object> build() throws CoreException {
    Map<String, Object> result = convertAndDecode(getConfig());
    result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
    return result;
  }

  private static Map<String, Object> convertAndDecode(KeyValuePairSet kvps) throws CoreException {
    Map<String, Object> result = new HashMap<>();
    try {
      for (KeyValuePair kvp : kvps.getKeyValuePairs()) {
        if (PASSWORD_KEYS.contains(kvp.getKey())) {
          result.put(kvp.getKey(), Password.decode(kvp.getValue()));
        } else {
          result.put(kvp.getKey(), kvp.getValue());
        }
      }
    } catch (PasswordException e) {
      throw new CoreException(e);
    }
    return result;
  }
}
