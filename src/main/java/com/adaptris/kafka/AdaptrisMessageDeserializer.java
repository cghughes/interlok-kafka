package com.adaptris.kafka;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

/**
 * Deserializer implementation for {@link AdaptrisMessage} objects.
 * <p>
 * Note that this simply preserves the byte[] payload, and no metadata for interoperability with other consumers and producers.
 * </p>
 * 
 * @author lchan
 *
 */
public class AdaptrisMessageDeserializer implements Deserializer<AdaptrisMessage> {

  /**
   * The metadata key that will store the topic.
   * 
   */
  public static final String KAFKA_TOPIC_KEY = "adpkafkatopic";

  private transient AdaptrisMessageFactory factory;

  public AdaptrisMessageDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    factory = defaultIfNull((AdaptrisMessageFactory) configs.get(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG));
  }

  @Override
  public void close() {}

  @Override
  public AdaptrisMessage deserialize(String topic, byte[] data) {
    AdaptrisMessage result = factory.newMessage(data);
    result.addMessageHeader(KAFKA_TOPIC_KEY, topic);
    return result;
  }

  AdaptrisMessageFactory messageFactory() {
    return factory;
  }

}
