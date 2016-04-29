package com.adaptris.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.common.config.SslConfigs;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public abstract class AdvancedConfigBuilderImpl implements ConfigBuilder {
  static final List<String> PASSWORD_KEYS = Arrays.asList(new String[] {SslConfigs.SSL_KEY_PASSWORD_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG});


  @NotNull
  @Valid
  private KeyValuePairSet config;


  public AdvancedConfigBuilderImpl() {
    setConfig(new KeyValuePairSet());
  }

  public AdvancedConfigBuilderImpl(KeyValuePairSet cfg) {
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

  protected static Map<String, Object> convertAndDecode(KeyValuePairSet kvps) throws CoreException {
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
