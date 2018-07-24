package com.adaptris.kafka;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;
import static com.adaptris.kafka.ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG;

import java.util.Map;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.MimeEncoder;

/**
 * Serializer implementation for {@link AdaptrisMessage} objects that uses {@link MimeEncoder}
 * <p>
 * Note the this serializer should be used in conjunction with {@link MimeEncoderSerializer}.
 * </p>
 * 
 * @since 3.8.0
 * 
 */
public class MimeEncoderSerializer extends AdaptrisMessageSerializer {

  private transient MimeEncoder encoder;

  public MimeEncoderSerializer() {
    this.encoder = new MimeEncoder();
    encoder.setRetainUniqueId(true);
  }


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    AdaptrisMessageFactory factory = defaultIfNull((AdaptrisMessageFactory) configs.get(KEY_DESERIALIZER_FACTORY_CONFIG));
    encoder.registerMessageFactory(factory);
  }

  @Override
  public byte[] serialize(String topic, AdaptrisMessage data) {
    try {
      return encoder.encode(data);
    }
    catch (CoreException e) {
      throw new RuntimeException(e);
    }
  }

}
