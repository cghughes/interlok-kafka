package com.adaptris.kafka;

import java.util.Map;

import com.adaptris.core.CoreException;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;

public interface ProducerConfigBuilder extends ConfigBuilder {

  default Map<String, Object> build() throws CoreException {
    return build(FilterKeys.Producer);
  }

}
