package com.adaptris.kafka;

import java.util.Map;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.MimeEncoder;

/**
 * Deserializer implementation for {@link AdaptrisMessage} objects that uses {@link MimeEncoder}.
 * <p>
 * Note the this deserializer should be used in conjunction with {@link MimeEncoderSerializer}.
 * </p>
 * 
 * @since 3.8.0
 */
public class MimeEncoderDeserializer extends AdaptrisMessageDeserializer {

  private transient MimeEncoder encoder;

  public MimeEncoderDeserializer() {
    this.encoder = new MimeEncoder();
    encoder.setRetainUniqueId(true);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    encoder.registerMessageFactory(factory);
  }

  @Override
  public AdaptrisMessage deserialize(String topic, byte[] data) {
    AdaptrisMessage result = null;
    try {
      result = encoder.decode(data);
      result.addMessageHeader(AdaptrisMessageDeserializer.KAFKA_TOPIC_KEY, topic);
    }
    catch (CoreException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

}
