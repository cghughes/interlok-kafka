package com.adaptris.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.common.config.SslConfigs;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ProducerConfigBuilder} that exposes all configuration.
 * 
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various
 * SSL passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be decoded using
 * {@link Password#decode(String)} appropriately.
 * </p>
 * 
 * @author lchan
 *
 */
@XStreamAlias("kafka-advanced-producer-config")
public class AdvancedProducerConfigBuilder implements ProducerConfigBuilder {

  private static final List<String> PASSWORD_KEYS = Arrays.asList(new String[] {SslConfigs.SSL_KEY_PASSWORD_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG});

  @NotNull
  @Valid
  private KeyValuePairSet config;


  public AdvancedProducerConfigBuilder() {
    setConfig(new KeyValuePairSet());
  }

  @Override
  public Properties build() throws CoreException {
    Properties result = new Properties();
    try {
      result = convertAndDecode(getConfig());
    } catch (PasswordException e) {
      ExceptionHelper.rethrowCoreException(e);
    }
    return result;
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

  private static Properties convertAndDecode(KeyValuePairSet kvps) throws PasswordException {
    Properties result = KeyValuePairBag.asProperties(kvps);
    for (String pwKey : PASSWORD_KEYS) {
      if (result.containsKey(pwKey)) {
        result.setProperty(pwKey, Password.decode(result.getProperty(pwKey)));
      }
    }
    return result;
  }
}
